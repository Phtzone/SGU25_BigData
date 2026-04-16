#!/usr/bin/env bash

resolve_python_bin() {
  if command -v python >/dev/null 2>&1; then
    echo "python"
  elif command -v python3 >/dev/null 2>&1; then
    echo "python3"
  else
    echo "Python interpreter not found. Install python3 or activate your virtual environment first." >&2
    return 1
  fi
}

create_artifact_dir() {
  mktemp -d
}

extract_json_field() {
  local field="$1"
  local required="${2:-0}"

  "$PYTHON_BIN" - "$field" "$required" <<'PY'
import json
import sys

field = sys.argv[1]
required = sys.argv[2] == "1"
text = sys.stdin.read()
decoder = json.JSONDecoder()
value_found = False
value = None
index = 0

while index < len(text):
    if text[index] not in "{[":
        index += 1
        continue
    try:
        payload, end = decoder.raw_decode(text, index)
    except json.JSONDecodeError:
        index += 1
        continue
    if isinstance(payload, dict) and field in payload:
        value = payload[field]
        value_found = True
    index = end

if not value_found:
    if required:
        raise SystemExit(f"Could not find JSON field {field!r} in command output")
    raise SystemExit(0)

if value is not None:
    print(value)
PY
}

read_output_path() {
  tr -d '\r\n' < "$1"
}

wait_for_kafka_listener() {
  "$PYTHON_BIN" - <<'PY'
import os
import socket
import time

host = os.getenv("KAFKA_WAIT_HOST", "localhost")
port = int(os.getenv("KAFKA_WAIT_PORT", "9093"))
timeout_seconds = float(os.getenv("KAFKA_STARTUP_TIMEOUT_SECONDS", "90"))
check_interval = float(os.getenv("KAFKA_STARTUP_CHECK_INTERVAL_SECONDS", "3"))
deadline = time.monotonic() + max(timeout_seconds, 0.0)
last_error = None

while time.monotonic() <= deadline:
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"Kafka listener is ready at {host}:{port}")
            raise SystemExit(0)
    except OSError as exc:
        last_error = exc
        time.sleep(max(check_interval, 0.1))

message = f"Timed out waiting for Kafka listener at {host}:{port}"
if last_error is not None:
    message += f" ({last_error})"
raise SystemExit(message)
PY
}
