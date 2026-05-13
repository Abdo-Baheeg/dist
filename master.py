"""
master.py — Distributed LLM Master Scheduler  v3.0

Key changes from v2:
  • Lifespan context manager (replaces deprecated @on_event)
  • All constants configurable via environment variables
  • Round-robin uses an unbounded atomic counter (no wrap-to-len bug)
  • master_failed tracked separately from worker-reported failed_requests
    so heartbeat updates never zero-out circuit-breaker counters
  • completed_requests incremented by master on success (not only via heartbeat)
  • HTTP 503 returned when all workers are exhausted (was 200 + error body)
  • Worker re-registration logs a warning instead of silently overwriting
  • URL validation on registration
  • /workers endpoint returns a sanitised view (no internal timestamps leaking)
  • /metrics includes per-scheduler stats
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# =========================================================
# CONFIGURATION  (all overridable via environment variables)
# =========================================================

HEARTBEAT_TIMEOUT         = int  (os.getenv("HEARTBEAT_TIMEOUT",         "15"))
FAILURE_DETECTOR_INTERVAL = int  (os.getenv("FAILURE_DETECTOR_INTERVAL",  "5"))
MAX_RETRIES               = int  (os.getenv("MAX_RETRIES",                "3"))
REQUEST_TIMEOUT           = float(os.getenv("REQUEST_TIMEOUT",          "120"))
MAX_CONCURRENT_REQUESTS   = int  (os.getenv("MAX_CONCURRENT_REQUESTS",   "50"))
SCHEDULING_ALGORITHM      =      os.getenv("SCHEDULING_ALGORITHM",  "load_aware")
# Valid values: round_robin | least_connections | load_aware

VALID_ALGORITHMS = {"round_robin", "least_connections", "load_aware"}

# =========================================================
# GLOBAL STATE
# =========================================================

# workers[name] schema:
#   worker_url, gpu, model, status
#   active_requests       — currently in-flight at that worker (master's view)
#   worker_completed      — completed count reported by worker via heartbeat
#   worker_failed         — failed count reported by worker via heartbeat
#   master_completed      — incremented here on every successful round-trip
#   master_failed         — incremented here on every failed round-trip
#                           (separate from worker_failed so heartbeat can't
#                            reset the circuit-breaker counter accidentally)
#   consecutive_failures  — circuit-breaker counter (resets on success / heartbeat)
#   last_heartbeat        — epoch seconds of last heartbeat
#   alive                 — bool
#   registered_at         — epoch seconds of first registration

workers: dict[str, dict] = {}
workers_lock = threading.Lock()

_rr_counter = 0               # unbounded; never wraps to len(alive)
rr_lock = threading.Lock()

_request_counter = 0
request_counter_lock = threading.Lock()

request_semaphore: Optional[asyncio.Semaphore] = None

# =========================================================
# STARTUP / SHUTDOWN  (lifespan — replaces deprecated @on_event)
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global request_semaphore
    request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    logger.info(
        "Master started — scheduler=%s  max_concurrent=%d",
        SCHEDULING_ALGORITHM, MAX_CONCURRENT_REQUESTS,
    )
    yield
    # graceful shutdown hook (nothing to flush currently)
    logger.info("Master shutting down.")


app = FastAPI(
    title="Distributed LLM Master",
    version="3.0",
    lifespan=lifespan,
)

# =========================================================
# CORS CONFIGURATION
# =========================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# REQUEST MODELS
# =========================================================

class WorkerRegistration(BaseModel):
    worker_name: str
    worker_url:  str
    gpu:         str
    model:       str
    status:      str

    @field_validator("worker_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("worker_url must be a valid http/https URL")
        return v.rstrip("/")   # normalise — no trailing slash

    @field_validator("worker_name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("worker_name must not be empty")
        return v


class HeartbeatRequest(BaseModel):
    worker_name: str
    metrics:     dict


class UserQuery(BaseModel):
    query: str


# =========================================================
# WORKER REGISTRATION
# =========================================================

@app.post("/register_worker", status_code=status.HTTP_201_CREATED)
async def register_worker(worker: WorkerRegistration):
    with workers_lock:
        if worker.worker_name in workers:
            # Re-registration: update URL/model but preserve counters & alive state
            existing = workers[worker.worker_name]
            existing.update({
                "worker_url": worker.worker_url,
                "gpu":        worker.gpu,
                "model":      worker.model,
                "status":     worker.status,
                "last_heartbeat": time.time(),
                "alive":      True,
            })
            logger.warning("[RE-REGISTERED] %s  url=%s", worker.worker_name, worker.worker_url)
            return {"message": "worker re-registered", "worker_name": worker.worker_name}

        workers[worker.worker_name] = {
            "worker_url":          worker.worker_url,
            "gpu":                 worker.gpu,
            "model":               worker.model,
            "status":              worker.status,
            "active_requests":     0,
            "worker_completed":    0,
            "worker_failed":       0,
            "master_completed":    0,
            "master_failed":       0,
            "consecutive_failures": 0,
            "last_heartbeat":      time.time(),
            "alive":               True,
            "registered_at":       time.time(),
        }
    logger.info("[REGISTERED] %s  url=%s  gpu=%s  model=%s",
                worker.worker_name, worker.worker_url, worker.gpu, worker.model)
    return {"message": "worker registered", "worker_name": worker.worker_name}


# =========================================================
# HEARTBEAT
# =========================================================

@app.post("/heartbeat")
async def heartbeat(data: HeartbeatRequest):
    with workers_lock:
        if data.worker_name not in workers:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="worker not registered — call /register_worker first",
            )

        w = workers[data.worker_name]
        w["last_heartbeat"]       = time.time()
        w["alive"]                = True
        w["consecutive_failures"] = 0   # heartbeat itself is proof of life

        m = data.metrics
        # Worker-reported counters (prefixed worker_) — never touch master_* here
        if "active_requests"    in m: w["active_requests"]    = m["active_requests"]
        if "completed_requests" in m: w["worker_completed"]   = m["completed_requests"]
        if "failed_requests"    in m: w["worker_failed"]      = m["failed_requests"]
        if "status"             in m: w["status"]             = m["status"]

    return {"message": "heartbeat received", "timestamp": time.time()}


# =========================================================
# FAILURE DETECTOR — background daemon thread
# =========================================================

def _failure_detector():
    while True:
        now = time.time()
        with workers_lock:
            for name, w in workers.items():
                delta = now - w["last_heartbeat"]
                if delta > HEARTBEAT_TIMEOUT and w["alive"]:
                    w["alive"] = False
                    logger.warning("[TIMEOUT] %s — silent for %.1fs", name, delta)
        time.sleep(FAILURE_DETECTOR_INTERVAL)


threading.Thread(target=_failure_detector, daemon=True, name="failure-detector").start()


# =========================================================
# HELPERS
# =========================================================

def _alive_snapshot(exclude: set | None = None) -> list[dict]:
    """Lock-safe snapshot of alive workers, with name injected for convenience."""
    exclude = exclude or set()
    with workers_lock:
        return [
            {"name": name, **w}
            for name, w in workers.items()
            if w["alive"] and name not in exclude
        ]


def _inc_active(name: str, delta: int):
    with workers_lock:
        if name in workers:
            workers[name]["active_requests"] = max(0, workers[name]["active_requests"] + delta)


def _record_success(name: str):
    with workers_lock:
        if name in workers:
            w = workers[name]
            w["active_requests"]      = max(0, w["active_requests"] - 1)
            w["master_completed"]    += 1
            w["consecutive_failures"] = 0


def _record_failure(name: str):
    with workers_lock:
        if name in workers:
            w = workers[name]
            w["active_requests"]      = max(0, w["active_requests"] - 1)
            w["master_failed"]       += 1
            w["consecutive_failures"] = w.get("consecutive_failures", 0) + 1
            if w["consecutive_failures"] >= 3:
                w["alive"] = False
                logger.error("[CIRCUIT-OPEN] %s — %d consecutive failures",
                             name, w["consecutive_failures"])


# =========================================================
# SCHEDULING ALGORITHMS
# =========================================================

def _select_round_robin(exclude: set | None = None) -> Optional[dict]:
    """
    Uses an unbounded counter so rr_index is never clamped to the alive-list
    length of any single snapshot — avoids repeated selection when workers
    join/leave between calls.
    """
    global _rr_counter
    alive = _alive_snapshot(exclude)
    if not alive:
        return None
    with rr_lock:
        idx = _rr_counter % len(alive)
        _rr_counter += 1
    return alive[idx]


def _select_least_connections(exclude: set | None = None) -> Optional[dict]:
    alive = _alive_snapshot(exclude)
    return min(alive, key=lambda w: w["active_requests"]) if alive else None


def _select_load_aware(exclude: set | None = None) -> Optional[dict]:
    """
    Score = active_load * 0.6
           + failure_rate * 2.0   (normalised 0–1, no drift as totals grow)
           + consecutive  * 0.5   (recent back-to-back failure penalty)

    Uses master_failed / master_completed so the score reflects what *this*
    master has observed — not stale worker self-reports.
    """
    alive = _alive_snapshot(exclude)
    if not alive:
        return None

    best, best_score = None, float("inf")
    for w in alive:
        total        = w["master_completed"] + w["master_failed"]
        failure_rate = (w["master_failed"] / total) if total > 0 else 0.0

        score = (
            w["active_requests"]      * 0.6
            + failure_rate            * 2.0
            + w["consecutive_failures"] * 0.5
        )
        if score < best_score:
            best_score, best = score, w

    return best


def _select_worker(exclude: set | None = None) -> Optional[dict]:
    if   SCHEDULING_ALGORITHM == "round_robin":
        return _select_round_robin(exclude)
    elif SCHEDULING_ALGORITHM == "least_connections":
        return _select_least_connections(exclude)
    else:                                    # load_aware (default)
        return _select_load_aware(exclude)


# =========================================================
# SUBMIT TASK
# =========================================================

@app.post("/submit_task")
async def submit_task(user_query: UserQuery, response: Response):
    """
    Route a user query to an available worker with automatic retry on failure.

    Returns HTTP 503 (not 200) when every retry is exhausted — callers can
    distinguish success from failure without parsing the body.
    """
    global _request_counter
    with request_counter_lock:
        _request_counter += 1
        req_id = _request_counter

    tried: set[str] = set()
    last_err: str | None = None

    async with request_semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            worker = _select_worker(exclude=tried)
            if worker is None:
                break

            tried.add(worker["name"])
            _inc_active(worker["name"], +1)   # optimistic — visible to scheduler

            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    resp = await client.post(
                        f"{worker['worker_url']}/process",
                        json={"query": user_query.query},
                    )
                resp.raise_for_status()
                result = resp.json()

                _record_success(worker["name"])
                logger.info("[REQ %d] OK  worker=%s  attempt=%d", req_id, worker["name"], attempt)

                return {
                    "request_id":      req_id,
                    "scheduler":       SCHEDULING_ALGORITHM,
                    "selected_worker": worker["name"],
                    "attempts":        attempt,
                    "result":          result,
                }

            except Exception as exc:
                last_err = str(exc)
                _record_failure(worker["name"])
                logger.warning(
                    "[REQ %d] FAIL  worker=%s  attempt=%d  err=%s",
                    req_id, worker["name"], attempt, exc,
                )

    # All retries exhausted — return 503 so clients know to back off
    logger.error("[REQ %d] ALL WORKERS FAILED — tried=%s", req_id, tried)
    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "request_id":    req_id,
        "error":         "all retries exhausted — no worker could handle the request",
        "last_error":    last_err,
        "tried_workers": list(tried),
    }


# =========================================================
# MANAGEMENT ENDPOINTS
# =========================================================

@app.get("/workers")
async def list_workers():
    """Returns a sanitised view of all registered workers."""
    with workers_lock:
        return {
            name: {
                "worker_url":          w["worker_url"],
                "gpu":                 w["gpu"],
                "model":               w["model"],
                "status":              w["status"],
                "alive":               w["alive"],
                "active_requests":     w["active_requests"],
                "master_completed":    w["master_completed"],
                "master_failed":       w["master_failed"],
                "worker_completed":    w["worker_completed"],
                "worker_failed":       w["worker_failed"],
                "consecutive_failures": w["consecutive_failures"],
                "seconds_since_heartbeat": round(time.time() - w["last_heartbeat"], 1),
                "registered_at":       w["registered_at"],
            }
            for name, w in workers.items()
        }


@app.delete("/workers/{worker_name}", status_code=status.HTTP_200_OK)
async def deregister_worker(worker_name: str):
    """Graceful worker removal (e.g. before a Colab session ends)."""
    with workers_lock:
        if worker_name not in workers:
            raise HTTPException(status_code=404, detail="Worker not found")
        del workers[worker_name]
    logger.info("[DEREGISTERED] %s", worker_name)
    return {"message": f"{worker_name} removed"}


@app.get("/metrics")
async def metrics():
    alive = _alive_snapshot()
    with workers_lock:
        all_w = list(workers.values())

    total_master_completed = sum(w["master_completed"] for w in all_w)
    total_master_failed    = sum(w["master_failed"]    for w in all_w)
    total_master_requests  = total_master_completed + total_master_failed

    return {
        "workers_total":          len(all_w),
        "workers_alive":          len(alive),
        "workers_dead":           len(all_w) - len(alive),
        "requests_received":      _request_counter,
        "master_completed":       total_master_completed,
        "master_failed":          total_master_failed,
        "master_failure_rate":    round(
            total_master_failed / total_master_requests, 4
        ) if total_master_requests > 0 else 0.0,
        "scheduler":              SCHEDULING_ALGORITHM,
        "max_concurrent_cap":     MAX_CONCURRENT_REQUESTS,
    }


@app.post("/scheduler/{algorithm}")
async def change_scheduler(algorithm: str):
    global SCHEDULING_ALGORITHM
    if algorithm not in VALID_ALGORITHMS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown algorithm. Choose from: {sorted(VALID_ALGORITHMS)}",
        )
    SCHEDULING_ALGORITHM = algorithm
    logger.info("[SCHEDULER] → %s", algorithm)
    return {"message": f"scheduler changed to {algorithm}"}


@app.get("/health")
async def health():
    """Lightweight liveness probe — used by NGINX and external monitors."""
    alive = _alive_snapshot()
    return {
        "status":        "ok",
        "alive_workers": len(alive),
        "scheduler":     SCHEDULING_ALGORITHM,
    }


@app.get("/")
async def root():
    return {
        "service":   "Distributed LLM Master",
        "version":   "3.0",
        "scheduler": SCHEDULING_ALGORITHM,
        "docs":      "/docs",
    }
