#!/bin/bash
# Veo Flow Worker - Quick Setup
# Usage: curl -sL https://veo-web-app-v3.onrender.com/api/user-worker/download/setup.sh | bash

set -e

echo "========================================"
echo "  Veo Flow Worker - Quick Setup"
echo "========================================"

MIN_PYTHON_MINOR=9

# Check Python
if ! command -v python3 &>/dev/null; then
    echo ""
    echo "Python 3 not found. Attempting to install..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if command -v brew &>/dev/null; then
            echo "Installing Python via Homebrew..."
            brew install python@3.12
        else
            echo "Please install Python first:"
            echo "  brew install python3"
            echo "  or download from https://python.org/downloads"
            exit 1
        fi
    else
        if command -v apt-get &>/dev/null; then
            echo "Installing Python via apt..."
            sudo apt-get update -qq && sudo apt-get install -y -qq python3 python3-pip python3-venv
        elif command -v dnf &>/dev/null; then
            echo "Installing Python via dnf..."
            sudo dnf install -y python3 python3-pip
        else
            echo "Please install Python first:"
            echo "  Download from https://python.org/downloads"
            exit 1
        fi
    fi
fi

PYTHON=$(command -v python3)
PY_VERSION=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MINOR=$($PYTHON -c "import sys; print(sys.version_info.minor)")

echo "Python: $($PYTHON --version)"

# Check if Python is too old
if [ "$PY_MINOR" -lt "$MIN_PYTHON_MINOR" ]; then
    echo "Python 3.${MIN_PYTHON_MINOR}+ required (you have 3.${PY_MINOR}). Attempting upgrade..."
    if [[ "$OSTYPE" == "darwin"* ]] && command -v brew &>/dev/null; then
        brew install python@3.12 || brew upgrade python@3.12
        if command -v python3.12 &>/dev/null; then
            PYTHON=$(command -v python3.12)
        else
            PYTHON=$(command -v python3)
        fi
    elif command -v apt-get &>/dev/null; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq python3.12 python3.12-venv 2>/dev/null || \
            sudo apt-get install -y -qq python3.11 python3.11-venv 2>/dev/null || \
            echo "Could not upgrade Python. Please install Python 3.9+ manually."
        if command -v python3.12 &>/dev/null; then
            PYTHON=$(command -v python3.12)
        elif command -v python3.11 &>/dev/null; then
            PYTHON=$(command -v python3.11)
        fi
    fi
    echo "Using: $($PYTHON --version)"
fi

# Ensure pip is available
$PYTHON -m ensurepip --upgrade 2>/dev/null || true
$PYTHON -m pip install --upgrade pip --quiet 2>/dev/null || true

# Pre-install requests so setup_worker.py can use it
echo "Installing requests..."
$PYTHON -m pip install --upgrade requests --quiet 2>/dev/null || \
    $PYTHON -m pip install --upgrade requests --quiet --user 2>/dev/null || \
    $PYTHON -m pip install --upgrade requests --quiet --break-system-packages 2>/dev/null || true

# Download setup script
SETUP_URL="https://veo-web-app-v3.onrender.com/api/user-worker/download/setup_worker.py"
SETUP_PATH="/tmp/veo_setup_worker.py"

echo ""
echo "Downloading setup script..."
curl -sL "$SETUP_URL" -o "$SETUP_PATH"

# Run setup INTERACTIVELY with /dev/tty for stdin
# This allows input() prompts to work even when this script is piped
echo ""
if [ -n "$VEO_TOKEN" ]; then
    $PYTHON "$SETUP_PATH" --token "$VEO_TOKEN" "$@" < /dev/tty
else
    $PYTHON "$SETUP_PATH" "$@" < /dev/tty
fi
