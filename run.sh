#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════
#  run.sh — 6-Node Ring KV Store on Minikube
#
#  Mirrors the benchmark/run_benchmark.sh style exactly.
#
#  Usage:
#    ./run.sh               # full deploy + demo
#    ./run.sh --crash-node2 # full deploy + demo + crash Node 2 mid-demo
#    ./run.sh --clean       # delete all resources and exit
#    ./run.sh --status      # print current cluster state
#
#  Prerequisites:
#    minikube start --driver=docker
#    kubectl
#    python3 with flask requests installed
# ════════════════════════════════════════════════════════════════
set -e

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m';  BOLD='\033[1m';     NC='\033[0m'

section() { echo -e "\n${CYAN}${BOLD}══ $1 ══${NC}"; }
ok()      { echo -e "${GREEN}✓${NC} $1"; }
info()    { echo -e "${YELLOW}→${NC} $1"; }
warn()    { echo -e "${RED}!${NC} $1"; }

# ── Config ────────────────────────────────────────────────────
ROUTER_PORT=9000
# One port per node (kv-0 → 9001 ... kv-5 → 9006)
NODE_PORTS=(9001 9002 9003 9004 9005 9006)
CRASH_NODE2=false

# ── Args ──────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --crash-node2) CRASH_NODE2=true ;;
    --clean)
      section "Cleanup"
      kubectl delete -f k8s/ --ignore-not-found -R
      ok "All resources deleted"
      exit 0
      ;;
    --status)
      section "Cluster Status"
      kubectl get pods -l app=kv-ring -o wide
      echo ""
      kubectl get pods -l app=kv-router -o wide
      exit 0
      ;;
  esac
done

kill_fwds() {
  kill "${FWD_PIDS[@]}" 2>/dev/null || true
}
trap kill_fwds EXIT
FWD_PIDS=()

# ── Preflight ─────────────────────────────────────────────────
section "Preflight"
kubectl cluster-info --context minikube &>/dev/null \
  && ok "minikube reachable" \
  || { warn "Run: minikube start --driver=docker"; exit 1; }

# ── Build images inside minikube's Docker daemon ──────────────
section "Building Images"
eval "$(minikube docker-env)"
docker build -t kv-node:latest   ./pods/node/
docker build -t kv-router:latest ./router/
ok "Images built: kv-node:latest  kv-router:latest"

# ── Tear down any previous run ────────────────────────────────
section "Cleaning Previous Resources"
kubectl delete -f k8s/ --ignore-not-found -R &>/dev/null || true
sleep 3
ok "Previous resources removed"

# ── Deploy StatefulSet ────────────────────────────────────────
section "Deploying 6-Node StatefulSet"
kubectl apply -f k8s/statefulset.yaml

# Patch kv-0 to be the seed pod (pre-loads keys on boot)
# We do this with a strategic merge patch so the base YAML stays clean.
kubectl patch statefulset kv --type=json -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/env/-",
    "value": {}
  }
]' &>/dev/null || true

# Simpler: use kubectl set env
kubectl set env statefulset/kv IS_SEED=false   # default all to false first

info "Waiting for all 6 pods to be ready..."
kubectl rollout status statefulset/kv --timeout=120s
ok "StatefulSet ready"

# ── Seed kv-0 with initial data ───────────────────────────────
section "Seeding Node 0"
# Port-forward kv-0 temporarily to seed it
kubectl port-forward pod/kv-0 19001:8080 &>/dev/null &
SEED_FWD=$!
FWD_PIDS+=($SEED_FWD)
sleep 2

info "Pre-loading keys into kv-0 primary slots..."
python3 - <<'PYEOF'
import hashlib, json
from urllib.request import urlopen, Request
from urllib.error import URLError

KEY_SPACE   = 10000
TOTAL_SLOTS = 12
NODE0_SLOTS = [0, 1]   # kv-0 primary slots

def key_slot(k):
    return int(hashlib.md5(k.encode()).hexdigest(), 16) % TOTAL_SLOTS

def post_json(url, payload):
    data = json.dumps(payload).encode()
    req  = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=2) as r:
        return r.read()

count = 0
for i in range(KEY_SPACE):
    k = f"key:{i}"
    s = key_slot(k)
    if s in NODE0_SLOTS:
        try:
            post_json("http://localhost:19001/set", {"slot": s, "value": f"val:{i}:seed"})
            count += 1
        except URLError:
            pass
print(f"Seeded {count} keys into kv-0 slots {NODE0_SLOTS}")
PYEOF
ok "kv-0 seeded"
kill $SEED_FWD 2>/dev/null || true
sleep 1

# ── Deploy Router ─────────────────────────────────────────────
section "Deploying Router"
kubectl apply -f k8s/router.yaml
kubectl rollout status deployment/kv-router --timeout=60s
ok "Router ready"

# ── Port-forwards ─────────────────────────────────────────────
section "Setting Up Port-Forwards"

kubectl port-forward svc/kv-router-svc ${ROUTER_PORT}:80 &>/dev/null &
FWD_PIDS+=($!)
info "Router  → localhost:${ROUTER_PORT}"

for i in "${!NODE_PORTS[@]}"; do
  PORT=${NODE_PORTS[$i]}
  kubectl port-forward pod/kv-${i} ${PORT}:8080 &>/dev/null &
  FWD_PIDS+=($!)
  info "kv-${i}  → localhost:${PORT}"
done

sleep 3

# Verify router is up
HEALTH=$(curl -s http://localhost:${ROUTER_PORT}/health 2>/dev/null || echo '{"status":"unreachable"}')
ok "Router health: ${HEALTH}"

# ── Run Demo ──────────────────────────────────────────────────
section "Running Demo"

NODE_URL_LIST=$(IFS=,; echo "${NODE_PORTS[*]/#/http://localhost:}")

python3 demo/demo.py \
  --router "http://localhost:${ROUTER_PORT}" \
  --nodes  "${NODE_URL_LIST}" \
  --node2  "http://localhost:${NODE_PORTS[2]}"

# (failover is now handled inside demo/demo.py — Phase 3)

# ── Summary ───────────────────────────────────────────────────
section "Summary"
echo ""
echo "  Ring KV store is running. Useful commands:"
echo ""
echo "  # Current pod state"
echo "  kubectl get pods -l app=kv-ring"
echo ""
echo "  # Router health"
echo "  curl http://localhost:${ROUTER_PORT}/health | python3 -m json.tool"
echo ""
echo "  # Slot ownership table"
echo "  curl http://localhost:${ROUTER_PORT}/slot_table | python3 -m json.tool"
echo ""
echo "  # Write a key"
echo "  curl -s -X POST http://localhost:${ROUTER_PORT}/set \\"
echo "       -H 'Content-Type: application/json' \\"
echo "       -d '{\"key\":\"mykey\",\"value\":\"myval\"}' | python3 -m json.tool"
echo ""
echo "  # Read a key"
echo "  curl http://localhost:${ROUTER_PORT}/get?key=mykey | python3 -m json.tool"
echo ""
echo "  # Crash a node and watch failover"
echo "  kubectl delete pod kv-2 --grace-period=0 --force"
echo "  sleep 2"
echo "  curl http://localhost:${ROUTER_PORT}/health | python3 -m json.tool"
echo ""
echo "  # Per-node stats"
for i in "${!NODE_PORTS[@]}"; do
  echo "  curl http://localhost:${NODE_PORTS[$i]}/stats | python3 -m json.tool   # kv-${i}"
done
echo ""
echo "  # Cleanup everything"
echo "  ./run.sh --clean"
echo ""

# Hold port-forwards alive until Ctrl+C
info "Port-forwards active. Press Ctrl+C to exit and kill them."
wait
