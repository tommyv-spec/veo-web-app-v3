#!/bin/bash
set -e

echo "============================================"
echo "  Veo Flow Worker - VPS Setup"
echo "============================================"
echo ""

echo "[1/6] Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
    python3 python3-pip python3-venv \
    xvfb xauth x11vnc \
    wget curl unzip \
    fonts-liberation fonts-noto-color-emoji \
    libnss3 libatk-bridge2.0-0 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 \
    libcups2 libatspi2.0-0 libgtk-3-0 \
    > /dev/null 2>&1 || true
echo "  ✓ Done"

echo ""
echo "[2/6] Installing Google Chrome..."
if command -v google-chrome-stable &> /dev/null; then
    echo "  ✓ Already installed ($(google-chrome-stable --version))"
else
    wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    sudo dpkg -i /tmp/chrome.deb 2>/dev/null || sudo apt-get install -f -y -qq > /dev/null 2>&1
    rm -f /tmp/chrome.deb
    echo "  ✓ Installed ($(google-chrome-stable --version))"
fi

echo ""
echo "[3/6] Installing Python packages..."
pip3 install --break-system-packages -q patchright requests 2>/dev/null || pip3 install -q patchright requests
python3 -m patchright install chromium > /dev/null 2>&1 || true
echo "  ✓ Done"

echo ""
echo "[4/6] Setting up worker directory..."
WORKER_DIR="$HOME/veo-worker"
mkdir -p "$WORKER_DIR/chrome-session" "$WORKER_DIR/chrome-download"

echo ""
echo "[5/6] Downloading worker..."
read -p "  Web app URL [https://veo-web-app-v3.onrender.com]: " WEB_APP_URL
WEB_APP_URL=${WEB_APP_URL:-https://veo-web-app-v3.onrender.com}
curl -sS "$WEB_APP_URL/api/user-worker/download/flow_worker.py" -o "$WORKER_DIR/flow_worker.py"
echo "  ✓ Downloaded"

echo ""
echo "[6/6] Configuration..."
echo "  1) User mode  — personal token (same as local)"
echo "  2) Admin mode — shared API key (your own VPS)"
read -p "  Mode [1]: " MODE_CHOICE
MODE_CHOICE=${MODE_CHOICE:-1}

if [ "$MODE_CHOICE" = "2" ]; then
    read -p "  LOCAL_WORKER_API_KEY [local-worker-secret-key-12345]: " API_KEY
    API_KEY=${API_KEY:-local-worker-secret-key-12345}
    cat > "$WORKER_DIR/.env" << EOF
WORKER_MODE=admin
LOCAL_WORKER_API_KEY=$API_KEY
WEB_APP_URL=$WEB_APP_URL
BROWSER_MODE=stealth
PROXY_TYPE=none
EOF
else
    read -p "  Worker token: " WORKER_TOKEN
    [ -z "$WORKER_TOKEN" ] && echo "Token required!" && exit 1
    cat > "$WORKER_DIR/.env" << EOF
WORKER_MODE=user
USER_WORKER_TOKEN=$WORKER_TOKEN
WEB_APP_URL=$WEB_APP_URL
BROWSER_MODE=stealth
PROXY_TYPE=none
EOF
fi
echo "  ✓ Config saved"

# --- run.sh ---
cat > "$WORKER_DIR/run.sh" << 'RUNSH'
#!/bin/bash
cd "$(dirname "$0")"
[ -f .env ] && set -a && source .env && set +a
export DISPLAY=:99
# Unbuffered, or worker.log lags reality by ~24 minutes and a working worker
# reads as hung. flow_worker.py prints its poll line in three places and only
# ONE of them flushes, so the output only looks live when stdout is a terminal.
# Here it is a PIPE into tee, which block-buffers harder than a file does --
# and the whole point of `tee -a worker.log` is a log someone can read.
# The discriminator, if you are ever staring at a silent log: ask whether the
# platform OWES clips (tools/launch_workers.py says). A quiet log is not
# evidence in either direction.
export PYTHONUNBUFFERED=1
pgrep -x Xvfb > /dev/null || { Xvfb :99 -screen 0 1920x1080x24 -ac & sleep 2; echo "✓ Xvfb started"; }
while true; do
    python3 flow_worker.py --single 2>&1 | tee -a worker.log
    echo "Exited. Restarting in 10s... (Ctrl+C to stop)"
    sleep 10
done
RUNSH
chmod +x "$WORKER_DIR/run.sh"

# --- login.sh (first-time Google login via VNC) ---
cat > "$WORKER_DIR/login.sh" << 'LOGINSH'
#!/bin/bash
cd "$(dirname "$0")"
export DISPLAY=:99
pgrep -x Xvfb > /dev/null || { Xvfb :99 -screen 0 1920x1080x24 -ac & sleep 2; }
pgrep -x x11vnc > /dev/null || { x11vnc -display :99 -nopw -forever > /dev/null 2>&1 & sleep 1; }
VPS_IP=$(curl -s ifconfig.me 2>/dev/null || echo "YOUR_VPS_IP")
echo ""
echo "  VNC running! Connect to: $VPS_IP:5900"
echo "  Then log into Google in the Chrome window."
echo "  Close Chrome when done."
echo ""
google-chrome-stable \
    --user-data-dir="$(pwd)/chrome-session" \
    --no-first-run --no-default-browser-check \
    "https://labs.google/fx/tools/flow" 2>/dev/null
echo "✓ Session saved. Run: bash run.sh"
LOGINSH
chmod +x "$WORKER_DIR/login.sh"

# --- systemd service ---
cat > "$WORKER_DIR/veo-worker.service" << SVCEOF
[Unit]
Description=Veo Flow Worker
After=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$WORKER_DIR
EnvironmentFile=$WORKER_DIR/.env
Environment=DISPLAY=:99
# Same reason as run.sh: systemd captures stdout through a pipe into the
# journal, so without this journalctl lags by ~24 minutes and a healthy worker
# looks dead. Only one of flow_worker.py three poll prints flushes.
Environment=PYTHONUNBUFFERED=1
ExecStartPre=/bin/bash -c 'pgrep Xvfb || Xvfb :99 -screen 0 1920x1080x24 -ac &'
ExecStartPre=/bin/sleep 2
ExecStart=/usr/bin/python3 $WORKER_DIR/flow_worker.py --single
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
SVCEOF

echo ""
echo "============================================"
echo "  ✓ Setup complete!"
echo "============================================"
echo ""
echo "  STEP 1 — First-time Google login (need VNC):"
echo "    cd $WORKER_DIR && bash login.sh"
echo "    Connect VNC client to VPS_IP:5900"
echo "    Log into Google → go to labs.google/fx → close Chrome"
echo ""
echo "  STEP 2 — Test run:"
echo "    cd $WORKER_DIR && bash run.sh"
echo ""
echo "  STEP 3 — Run as service:"
echo "    sudo cp $WORKER_DIR/veo-worker.service /etc/systemd/system/"
echo "    sudo systemctl daemon-reload"
echo "    sudo systemctl enable --now veo-worker"
echo "    sudo journalctl -u veo-worker -f"
echo ""
