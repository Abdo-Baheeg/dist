# Distributed LLM Worker Node System

A distributed inference system that simulates multiple virtual LLM workers on a single GPU and routes queries via a master scheduler on AWS EC2.

## Overview

- **Master (AWS EC2)**: Central scheduler that receives queries, selects workers via round-robin/least-connections/load-aware algorithms, and handles retries.
- **Workers (Colab GPU)**: Ray actors running FastAPI servers, each with embedded FAISS-based RAG and concurrent Ollama LLM access.
- **Communication**: HTTP via Cloudflare tunnels; heartbeats keep the master aware of worker status.

## Architecture

```
Master (EC2)
    ↓ HTTP
    ├─→ Cloudflare tunnel ─→ Worker-1 (Ray actor + FastAPI)
    │                        ├─ RAG retriever (FAISS)
    │                        └─ Ollama LLM (shared)
    ├─→ Cloudflare tunnel ─→ Worker-2 (Ray actor + FastAPI)
    │                        ├─ RAG retriever (FAISS)
    │                        └─ Ollama LLM (shared)
    └─→ ...
```

## Prerequisites

- **Master**: AWS EC2 instance (t3.medium+), Python 3.9+, FastAPI, NGINX
- **Workers**: Google Colab with GPU, Python 3.9+, Ray, FastAPI, Ollama, sentence-transformers, FAISS
- **Networking**: Cloudflare (cloudflared CLI for tunnels; no account needed for quick tunnels)

## Quick Start

### 1. Start the Master (AWS EC2)

```bash
cd dist
python master.py
# Runs on http://<EC2_IP>:8000
# Configure via environment variables:
#   HEARTBEAT_TIMEOUT=15
#   SCHEDULING_ALGORITHM=load_aware
```

### 2. Run a Worker (Colab Notebook)

1. Upload `worker_v2.ipynb` to Google Colab
2. Set `AWS_SERVER_IP` to your EC2 public IP
3. Run cells sequentially:
   - **Cell 1**: Configuration
   - **Cell 2**: Install dependencies & Ollama
   - **Cell 3**: Start Ollama server & pull model
   - **Cell 4**: Initialize Ray
   - **Cell 5–7**: Spawn virtual workers, start Cloudflare tunnels, register with master
   - **Cell 8**: Start Cloudflare tunnels (one per worker)
- **Cell 9–14**: Run tests and monitoring

### 3. Send Queries

```bash
curl -X POST http://<EC2_IP>:8000/submit_task \
  -H "Content-Type: application/json" \
  -d '{"query": "What is distributed computing?"}'
```

Or test locally from the Colab notebook (Cell 13–15).

## Key Features

- **Multi-worker simulation**: Spawn N virtual workers on a single GPU
- **Automatic retry**: Master retries failed queries on other workers
- **Fault detection**: Heartbeat timeout marks workers as dead; circuit-breaker stops repeated failures
- **Three schedulers**:
  - `round_robin`: Distribute evenly
  - `least_connections`: Route to least-busy worker
  - `load_aware`: Score workers by load, failures, and latency
- **RAG pipeline**: Workers retrieve context from knowledge base before generating answers
- **Async FastAPI**: Requests queue instead of being rejected
- **Metrics tracking**: Per-worker success/failure counts, latency, active connections

## File Structure

```
dist/
├── master.py                 # Master scheduler on AWS EC2
├── worker_v2.ipynb          # Colab notebook with virtual workers
├── README.md                # This file
└── notebooks/
    └── worker_v2.ipynb      # Alternative path
```

## Configuration

### Master (master.py)
Environment variables (defaults shown):
```bash
HEARTBEAT_TIMEOUT=15              # seconds before marking worker dead
FAILURE_DETECTOR_INTERVAL=5       # how often to check for timeouts
MAX_RETRIES=3                     # query retries before 503
REQUEST_TIMEOUT=120               # seconds per HTTP call
MAX_CONCURRENT_REQUESTS=50        # semaphore cap
SCHEDULING_ALGORITHM=load_aware   # round_robin | least_connections | load_aware
```

### Worker (Colab)
Edit in Cell 1:
```python
AWS_SERVER_IP = "13.63.238.244"   # Master EC2 IP
MODEL_NAME = "qwen2:0.5b"         # Ollama model
NUM_VIRTUAL_WORKERS = 3           # Actors per GPU
BASE_PORT = 8001                  # First worker port
HEARTBEAT_SECONDS = 8             # Heartbeat interval
```

## Monitoring

- **Master dashboard**: `http://<EC2_IP>:8000/docs` (FastAPI Swagger UI)
- **Metrics**: `GET /metrics` — returns worker counts, request totals, failure rates
- **Worker status**: `GET /workers` — per-worker metrics, alive state, latency
- **Colab dashboard**: Cell 12 shows real-time local + master status

## Scheduling Algorithms

| Algorithm | Behavior | Best For |
|-----------|----------|----------|
| **round_robin** | Cycle through workers | Equal load |
| **least_connections** | Pick worker with fewest active requests | Bursty workloads |
| **load_aware** | Score by active_requests, failure_rate, consecutive_failures | Mixed workloads |

Switch at runtime:
```bash
curl -X POST http://<EC2_IP>:8000/scheduler/load_aware
```

## Troubleshooting

- **Workers not registering**: Check EC2 security groups allow port 8000; verify `AWS_SERVER_IP`
- **High latency**: Model may be cold; run warmup queries (Cell 15 demo)
- **Heartbeat timeouts**: Increase `HEARTBEAT_TIMEOUT` if Ollama is slow
- **Cloudflare tunnel connection fails**: Quick tunnels are ephemeral; if cloudflared crashes, restart Cell 8 to get new URLs

## Example Workflow

1. Cell 1: Set master IP and model
2. Cells 2–7: Install, start Ollama, spawn workers
3. Cell 12: Dashboard shows registration
4. Cell 13: Test local actor directly
5. Cell 14: Test via master (end-to-end)
6. Cell 15: Run 120-request load test, watch distribution
7. Cell 16: Simulate worker failure, show automatic failover
8. Cell 18: Graceful shutdown before ending Colab session

## Performance Tips

- **Queue depth** (Cell 9): Increase from 6 to 12 if you have many incoming requests
- **GPU fraction**: Reduce per-worker fraction to fit more workers (`GPU_FRACTION_PER_WORKER`)
- **Model size**: Start with `qwen2:0.5b` or `phi3:mini`; larger models require more VRAM
- **Scheduling**: Use `load_aware` for best latency distribution

## License

CSE362 Distributed Computing Project — Educational use.
