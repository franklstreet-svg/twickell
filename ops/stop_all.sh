#!/usr/bin/env bash
set -u

cd /home/frank/projects/TWICKELL_ROOT 2>/dev/null || { echo "TWICKELL_ROOT_MISSING"; exit 0; }

# Stop worker(s)
pkill -f "workers/queue_worker.py" >/dev/null 2>&1 || true

# Stop API
pkill -f "uvicorn app.app:app" >/dev/null 2>&1 || true

sleep 1

echo "PORT_8111:"
ss -ltnp | grep ":8111" || echo "8111_NOT_LISTENING"

echo "WORKER:"
pgrep -af "workers/queue_worker.py" || echo "WORKER_NOT_RUNNING"

echo "STOP_ALL_DONE"
