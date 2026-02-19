#!/usr/bin/env bash
set -u

cd /home/frank/projects/TWICKELL_ROOT 2>/dev/null || { echo "TWICKELL_ROOT_MISSING"; return 0 2>/dev/null || exit 0; }

# If already running, do nothing.
if pgrep -af "workers/queue_worker.py" >/dev/null 2>&1; then
  echo "WORKER_ALREADY_RUNNING"
  pgrep -af "workers/queue_worker.py"
  exit 0
fi

# Start one worker
nohup python3 -u workers/queue_worker.py >> logs/queue_worker.log 2>&1 &
sleep 1

echo "WORKER_STARTED"
pgrep -af "workers/queue_worker.py" || echo "WORKER_NOT_RUNNING"
