#!/usr/bin/env bash
# Deploy Foghorn to the Hetzner VPS.
# Usage: ./deploy.sh
#
# Foghorn is self-contained: docker-compose bundles its own Postgres (the `db`
# service + `foghorn_pgdata` volume), so there is no dependency on any external
# database or docker network. Migrations are baked into the binaries via
# sqlx::migrate! and run automatically on startup.
#
# NOTE: config.toml is intentionally excluded from rsync (it holds the gateway
# API key + RPC URL). It must already exist at $REMOTE_DIR/config.toml on the VPS.
set -euo pipefail

VPS="root@167.235.29.213"
SSH_KEY="$HOME/.ssh/hetzner_drpc"
REMOTE_DIR="/root/foghorn"

echo "==> Syncing source to VPS..."
rsync -avz --exclude target --exclude .git --exclude node_modules --exclude config.toml \
  -e "ssh -i $SSH_KEY" \
  "$(dirname "$0")/" "$VPS:$REMOTE_DIR/"

if ! ssh -i "$SSH_KEY" "$VPS" "test -f $REMOTE_DIR/config.toml"; then
  echo "ERROR: $REMOTE_DIR/config.toml missing on VPS. Create it first (see config.example.toml)." >&2
  exit 1
fi

echo "==> Building and starting Foghorn containers..."
ssh -i "$SSH_KEY" "$VPS" \
  "cd $REMOTE_DIR && docker compose build && docker compose up -d"

echo "==> Status:"
ssh -i "$SSH_KEY" "$VPS" \
  "cd $REMOTE_DIR && docker compose ps"

echo ""
echo "Foghorn API is available at http://167.235.29.213:8082/v1/health"
echo "Set FOGHORN_API_URL=http://167.235.29.213:8082 in your Lodestar environment."
