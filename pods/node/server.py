"""
Ring KV Node Server

Each pod in the StatefulSet runs this. Identity comes from the
K8s pod name: kv-0 .. kv-5.

Ring topology (12 slots, 6 nodes, 2 primary each):
  Node i → primary slots [2i, 2i+1]
  Node (i+1)%6 holds the secondary copies of Node i's primaries

ENV vars:
  NODE_ID          int 0-5  (parsed from POD_NAME kv-{id})
  TOTAL_NODES      6
  NAMESPACE        default
  WAL_FLUSH_MS     100
  KEY_SPACE        10000  (pre-seed keys for seed node)
  IS_SEED          true/false

Protocol (HTTP/JSON):
  GET  /health
  GET  /get?slot=<n>          → primary read
  GET  /getsec?slot=<n>       → secondary read
  POST /set        {slot, value}           → primary write + WAL queued
  POST /rep        {slot, ver, value}      → replication write (LWW by ver)
  POST /promote    {slots: [n, ...]}       → secondary → primary promotion
  GET  /stats
  GET  /list_slots             → list primary slots this node owns
"""
import os, time, threading, hashlib, json, requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# ── Identity ──────────────────────────────────────────────────
POD_NAME     = os.environ.get("POD_NAME", "kv-0")
NODE_ID      = int(POD_NAME.split("-")[-1])
TOTAL_NODES  = int(os.environ.get("TOTAL_NODES", "6"))
TOTAL_SLOTS  = 12
SLOTS_PER    = TOTAL_SLOTS // TOTAL_NODES   # 2
NAMESPACE    = os.environ.get("NAMESPACE", "default")
WAL_FLUSH_MS = int(os.environ.get("WAL_FLUSH_MS", "100"))
KEY_SPACE    = int(os.environ.get("KEY_SPACE", "10000"))
IS_SEED      = os.environ.get("IS_SEED", "false").lower() == "true"

# ── Ring topology ─────────────────────────────────────────────
primary_slots   = list(range(NODE_ID * SLOTS_PER, NODE_ID * SLOTS_PER + SLOTS_PER))
source_id       = (NODE_ID - 1) % TOTAL_NODES          # whose secondaries we hold
secondary_slots = list(range(source_id * SLOTS_PER, source_id * SLOTS_PER + SLOTS_PER))
buddy_id        = (NODE_ID + 1) % TOTAL_NODES           # who holds OUR secondaries

def buddy_url():
    return f"http://kv-{buddy_id}.kv-headless.{NAMESPACE}.svc.cluster.local:8080"

# ── Storage ───────────────────────────────────────────────────
pstore = {}    # {slot: {val, ver}}  primary
sstore = {}    # {slot: {val, ver}}  secondary
plock  = threading.RLock()
slock  = threading.RLock()

# ── WAL ───────────────────────────────────────────────────────
wal      = []
wal_lock = threading.Lock()
wal_cv   = threading.Condition(wal_lock)

stats = {
    "requests": 0, "pri_reads": 0, "sec_reads": 0,
    "writes": 0, "wal_flushed": 0, "rep_received": 0,
    "promotions": 0, "latencies_ms": [],
    "stale_fallbacks": 0,
}
stats_lock = threading.Lock()

def record_lat(ms):
    with stats_lock:
        stats["latencies_ms"].append(ms)
        if len(stats["latencies_ms"]) > 2000:
            stats["latencies_ms"].pop(0)

# ── WAL flush thread ──────────────────────────────────────────
def wal_flush_loop():
    while True:
        with wal_cv:
            wal_cv.wait_for(lambda: len(wal) > 0, timeout=WAL_FLUSH_MS / 1000)
            batch, wal[:] = wal[:], []

        for rec in batch:
            try:
                requests.post(
                    f"{buddy_url()}/rep",
                    json=rec, timeout=1
                )
                with stats_lock:
                    stats["wal_flushed"] += 1
            except Exception:
                pass  # buddy down — degraded, router already knows

threading.Thread(target=wal_flush_loop, daemon=True).start()

# ── Routes ────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "pod": POD_NAME,
        "node_id": NODE_ID,
        "primary_slots": primary_slots,
        "secondary_slots": secondary_slots,
        "buddy_id": buddy_id,
        "source_id": source_id,
        "pri_keys": len(pstore),
        "sec_keys": len(sstore),
    }), 200


@app.route("/get")
def get_primary():
    t0   = time.time()
    slot = int(request.args.get("slot", -1))

    with stats_lock:
        stats["requests"] += 1
        stats["pri_reads"] += 1

    with plock:
        entry = pstore.get(slot)

    lat = (time.time() - t0) * 1000
    record_lat(lat)

    if entry is None:
        return jsonify({"hit": False, "slot": slot, "pod": POD_NAME,
                        "latency_ms": round(lat, 2)}), 404
    return jsonify({"hit": True, "slot": slot, "value": entry["val"],
                    "ver": entry["ver"], "pod": POD_NAME,
                    "latency_ms": round(lat, 2)})


@app.route("/getsec")
def get_secondary():
    t0   = time.time()
    slot = int(request.args.get("slot", -1))

    with stats_lock:
        stats["requests"] += 1
        stats["sec_reads"] += 1

    with slock:
        entry = sstore.get(slot)

    lat = (time.time() - t0) * 1000
    record_lat(lat)

    if entry is None:
        return jsonify({"hit": False, "slot": slot, "pod": POD_NAME,
                        "latency_ms": round(lat, 2)}), 404
    return jsonify({"hit": True, "slot": slot, "value": entry["val"],
                    "ver": entry["ver"], "pod": POD_NAME,
                    "source": "secondary", "latency_ms": round(lat, 2)})


@app.route("/set", methods=["POST"])
def set_key():
    t0   = time.time()
    data = request.json
    slot = int(data["slot"])
    val  = data["value"]

    with plock:
        cur_ver = pstore[slot]["ver"] if slot in pstore else 0
        new_ver = cur_ver + 1
        pstore[slot] = {"val": val, "ver": new_ver}

    # Queue to WAL for async buddy replication
    with wal_cv:
        wal.append({"slot": slot, "ver": new_ver, "value": val})
        wal_cv.notify()

    with stats_lock:
        stats["writes"] += 1

    lat = (time.time() - t0) * 1000
    record_lat(lat)
    return jsonify({"ok": True, "slot": slot, "ver": new_ver,
                    "pod": POD_NAME, "latency_ms": round(lat, 2)})


@app.route("/rep", methods=["POST"])
def replicate():
    """Called by source node's WAL flush. Last-write-wins by version."""
    data = request.json
    slot = int(data["slot"])
    ver  = int(data["ver"])
    val  = data["value"]

    with slock:
        cur = sstore.get(slot, {})
        if ver > cur.get("ver", 0):
            sstore[slot] = {"val": val, "ver": ver}

    with stats_lock:
        stats["rep_received"] += 1

    return jsonify({"ok": True}), 200


@app.route("/promote", methods=["POST"])
def promote():
    """
    Router calls this during failover when our source node dies.
    We move the specified secondary slots into primary storage.
    """
    slots = request.json.get("slots", [])
    promoted = []

    with slock:
        with plock:
            for s in slots:
                s = int(s)
                if s in sstore:
                    pstore[s] = sstore.pop(s)
                    promoted.append(s)
                    if s not in primary_slots:
                        primary_slots.append(s)
                    if s in secondary_slots:
                        secondary_slots.remove(s)

    with stats_lock:
        stats["promotions"] += len(promoted)

    return jsonify({"ok": True, "promoted": promoted, "pod": POD_NAME}), 200


@app.route("/list_slots")
def list_slots():
    return jsonify({
        "primary_slots": primary_slots,
        "secondary_slots": secondary_slots,
        "pri_keys": list(pstore.keys()),
        "sec_keys": list(sstore.keys()),
    })


@app.route("/stats")
def get_stats():
    with stats_lock:
        lats = stats["latencies_ms"]
        sl   = sorted(lats) if lats else [0]
        return jsonify({
            "pod": POD_NAME, "node_id": NODE_ID,
            "primary_slots": primary_slots,
            "secondary_slots": secondary_slots,
            "buddy_id": buddy_id,
            "requests": stats["requests"],
            "pri_reads": stats["pri_reads"],
            "sec_reads": stats["sec_reads"],
            "writes": stats["writes"],
            "wal_flushed": stats["wal_flushed"],
            "rep_received": stats["rep_received"],
            "promotions": stats["promotions"],
            "stale_fallbacks": stats["stale_fallbacks"],
            "pri_keys": len(pstore),
            "sec_keys": len(sstore),
            "p50_ms": round(sl[int(len(sl) * 0.50)], 2),
            "p95_ms": round(sl[int(len(sl) * 0.95)], 2),
            "p99_ms": round(sl[min(int(len(sl) * 0.99), len(sl) - 1)], 2),
        })


# ── Seed preload ──────────────────────────────────────────────
if IS_SEED:
    for i in range(KEY_SPACE):
        slot = i % TOTAL_SLOTS
        if slot in primary_slots:
            pstore[slot] = {"val": f"val:{i}:seed", "ver": 1}
    print(f"[{POD_NAME}] Seed loaded {len(pstore)} keys across slots {primary_slots}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
