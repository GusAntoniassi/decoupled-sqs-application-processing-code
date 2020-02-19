#!/bin/bash

if ! command -v python >/dev/null 2>&1; then
    echo "Python is not installed"
    exit 1
fi

cd "$(dirname "$0")"
pip3 install -r requirements.txt
python3 -u main.py
