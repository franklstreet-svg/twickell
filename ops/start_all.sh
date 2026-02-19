#!/usr/bin/env bash
set -u

cd /home/frank/projects/TWICKELL_ROOT 2>/dev/null || { echo "TWICKELL_ROOT_MISSING"; exit 0; }

# --- Twickell API (8111) ---
if ss -ltnp 2>/dev/null | grep -q ":8111"; then
  echo "API_ALREADY_LISTENING_8111"
else
  pkill -f "uvicorn app.app:app" >/dev/null 2>&1 || true
  nohup ./venv/bin/uvicorn app.app:app --host 127.0.0.1 --port 8111 >> logs/uvicorn_8111.log 2>&1 &
  sleep 1
fi

# --- Worker (singleton) ---
/home/frank/projects/TWICKELL_ROOT/ops/start_worker_singleton.sh

# --- Status ---
echo "PORT_8111:"
ss -ltnp | grep ":8111" || echo "8111_NOT_LISTENING"

echo "WORKER:"
pgrep -af "workers/queue_worker.py" || echo "WORKER_NOT_RUNNING"

echo "HEALTH:"
curl -s http://127.0.0.1:8111/health; echo
curl -s http://127.0.0.1:8111/bridge/health; echo
