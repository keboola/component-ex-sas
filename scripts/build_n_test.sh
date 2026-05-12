#!/bin/sh
set -e

ruff check src/ tests/
python -m pytest tests/ -v
