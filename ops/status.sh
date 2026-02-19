#!/usr/bin/env bash
set -u

cd /home/frank/projects/TWICKELL_ROOT 2>/dev/null || { echo "TWICKELL_ROOT_MISSING"; exit 0; }

echo "PORT_8111:"
ss -ltnp | grep ":8111" || echo "8111_NOT_LISTENING"

echo "WORKER:"
pgrep -af "workers/queue_worker.py" || echo "WORKER_NOT_RUNNING"

echo "HEALTH:"
curl -s http://127.0.0.1:8111/health 2>/dev/null || echo "HEALTH_FAIL"
echo
