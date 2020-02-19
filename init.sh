#!/bin/bash

if ! command -v python >/dev/null 2>&1; then
    echo "Python is not installed"
fi

cd "$(dirname "$0")"
source venv/bin/activate
python3 main.py