#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
export PATH="$PWD/.venv/bin:$PATH"
exec .venv/bin/python server.py
