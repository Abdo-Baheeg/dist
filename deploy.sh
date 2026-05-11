#!/bin/bash
# =========================================================
# deploy.sh — run once on your AWS EC2 instance
#
# Usage:
#   chmod +x deploy.sh
#   sudo ./deploy.sh
# =========================================================
set -e

APP_DIR="/home/ubuntu/llm-master"
SERVICE="llm-master"

echo "==> Creating app directory"
mkdir -p "$APP_DIR"
cp master.py "$APP_DIR/"

echo "==> Installing Python dependencies"
pip3 install fastapi uvicorn httpx --break-system-packages -q

# ---- Single master service (port 8000) ----
# WHY one service?
#   master.py keeps worker state in memory.  A second instance would have
#   its own empty registry — workers that registered to instance A are
#   invisible to instance B, causing every other request to fail.
#   One async uvicorn process handles concurrency without this split-brain.
echo "==> Installing $SERVICE.service"
cp llm-master.service /etc/systemd/system/${SERVICE}.service

# ---- NGINX config ----
echo "==> Installing NGINX config"
cp llm-master.conf /etc/nginx/conf.d/llm-master.conf
nginx -t    # validate — exits non-zero if config is broken

# ---- Remove legacy replica service if it exists ----
if systemctl is-active --quiet llm-master-replica 2>/dev/null; then
    echo "==> Stopping legacy llm-master-replica service"
    systemctl stop llm-master-replica
    systemctl disable llm-master-replica
fi
if [ -f /etc/systemd/system/llm-master-replica.service ]; then
    echo "==> Removing legacy llm-master-replica.service file"
    rm /etc/systemd/system/llm-master-replica.service
fi

# ---- Enable and start ----
echo "==> Enabling and starting $SERVICE"
systemctl daemon-reload
systemctl enable  ${SERVICE}
systemctl restart ${SERVICE}
systemctl reload  nginx

echo ""
echo "✓ Done. Status:"
systemctl is-active ${SERVICE} && echo "  ${SERVICE}: running on :8000"
echo ""
echo "Useful commands:"
echo "  sudo journalctl -u ${SERVICE} -f     ← live logs"
echo "  curl http://localhost/health          ← health check via NGINX"
echo "  curl http://localhost/metrics         ← request/worker stats"
echo "  curl http://localhost/workers         ← registered worker list"
echo "  sudo systemctl status ${SERVICE}     ← full status"
