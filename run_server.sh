#!/usr/bin/env bash
set -u

SERVER_NAME="Vidloq Stream Server V6.3 README Update"
HTTP_PORT=8000
AUDIO_TCP_PORT=8001
RESTART_SECONDS=3600
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
REQUIREMENTS_FILE="$PROJECT_DIR/requirements.txt"

find_python() {
  if command -v python3.12 >/dev/null 2>&1; then
    command -v python3.12
  elif [ -x /usr/local/bin/python3.12 ]; then
    echo /usr/local/bin/python3.12
  elif command -v python3 >/dev/null 2>&1; then
    command -v python3
  else
    echo ""
  fi
}

venv_has_bad_interpreter() {
  if [ ! -x "$VENV_DIR/bin/python" ]; then
    return 0
  fi
  if [ -f "$VENV_DIR/bin/uvicorn" ]; then
    first_line="$(head -n 1 "$VENV_DIR/bin/uvicorn" 2>/dev/null || true)"
    case "$first_line" in
      \#!*)
        interpreter="${first_line#\#!}"
        interpreter="${interpreter%% *}"
        if [ -n "$interpreter" ] && [ ! -x "$interpreter" ]; then
          echo "Detected stale uvicorn interpreter: $interpreter"
          return 0
        fi
        ;;
    esac
  fi
  return 1
}

ensure_venv() {
  py="$(find_python)"
  if [ -z "$py" ]; then
    echo "ERROR: python3.12 or python3 was not found."
    exit 1
  fi

  if venv_has_bad_interpreter; then
    echo "Removing stale or broken virtual environment: $VENV_DIR"
    rm -rf "$VENV_DIR"
  fi

  if [ ! -x "$VENV_DIR/bin/python" ]; then
    echo "Creating virtual environment with $py"
    "$py" -m venv "$VENV_DIR"
  fi

  echo "Using virtual environment: $VENV_DIR"
  "$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
  "$VENV_DIR/bin/python" -m pip install -r "$REQUIREMENTS_FILE"
}

kill_port_listeners() {
  for port in "$HTTP_PORT" "$AUDIO_TCP_PORT"; do
    pids=$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
    if [ -n "$pids" ]; then
      echo "Stopping old process(es) on port $port: $pids"
      kill $pids 2>/dev/null || true
      sleep 1
      still=$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
      if [ -n "$still" ]; then
        echo "Force stopping old process(es) on port $port: $still"
        kill -9 $still 2>/dev/null || true
      fi
    fi
  done
}

server_pid=""
shutdown() {
  echo "Stopping $SERVER_NAME"
  if [ -n "${server_pid:-}" ] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  exit 0
}
trap shutdown INT TERM

cd "$PROJECT_DIR" || exit 1
ensure_venv

echo "Starting $SERVER_NAME"
echo "HTTP: 0.0.0.0:$HTTP_PORT"
echo "Raw audio TCP ingest: 0.0.0.0:$AUDIO_TCP_PORT"
echo "Automatic restart interval: every $RESTART_SECONDS seconds"
echo "Venv-safe launch: using python -m uvicorn to avoid stale script shebangs"

while true; do
  kill_port_listeners
  echo "Launching server at $(date)"
  "$VENV_DIR/bin/python" -m uvicorn main:app --host 0.0.0.0 --port "$HTTP_PORT" &
  server_pid=$!
  start_time=$(date +%s)

  while kill -0 "$server_pid" 2>/dev/null; do
    now_time=$(date +%s)
    elapsed=$((now_time - start_time))
    if [ "$elapsed" -ge "$RESTART_SECONDS" ]; then
      echo "Hourly restart triggered at $(date). Stopping PID $server_pid"
      kill "$server_pid" 2>/dev/null || true
      wait "$server_pid" 2>/dev/null || true
      break
    fi
    sleep 5
  done

  wait "$server_pid" 2>/dev/null || true
  echo "Server stopped. Restarting in 2 seconds..."
  sleep 2
done
