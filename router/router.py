"""
Ring KV Router

Responsibilities:
  1. Key → slot mapping  (hash(key) % 12)
  2. Slot → node routing  (primary + secondary eligible set)
  3. Least-conn selection across eligible nodes
  4. Heartbeat monitoring  (every HB_TICK_MS, timeout = 3 missed ticks)
  5. Failover coordination (promote buddy, mark slot degraded)
  6. Stale secondary fallback on read miss

Topology (12 slots, 6 nodes):
  Node i → primary slots [2i, 2i+1]
  Node (i+1)%6 → secondary for node i

ENV:
  TOTAL_NODES   6
  NAMESPACE     default
  HB_TICK_MS    300
  HB_TIMEOUT    900   (ms, 3 missed ticks)
"""
import os, time, hashlib, threading, requests
from flask import Flask, jsonify, request

app = Flask(__name__)

TOTAL_NODES  = int(os.environ.get("TOTAL_NODES", "6"))
TOTAL_SLOTS  = 12
SLOTS_PER    = TOTAL_SLOTS // TOTAL_NODES
NAMESPACE    = os.environ.get("NAMESPACE", "default")
HB_TICK_MS   = int(os.environ.get("HB_TICK_MS", "300"))
HB_TIMEOUT   = int(os.environ.get("HB_TIMEOUT", "900"))

def node_url(i):
    return f"http://kv-{i}.kv-headless.{NAMESPACE}.svc.cluster.local:8080"

# ── Cluster state ─────────────────────────────────────────────
lock = threading.RLock()

node_alive   = [True] * TOTAL_NODES   # live/dead per node
node_conns   = [0]    * TOTAL_NODES   # active connection count (least-conn)
last_hb      = [time.time()] * TOTAL_NODES
failover_done = [False] * TOTAL_NODES

# slot_owner[s]  = primary node index for slot s
# slot_buddy[s]  = secondary node index for slot s (-1 = DEGRADED)
slot_owner = [i // SLOTS_PER for i in range(TOTAL_SLOTS)]
slot_buddy = [(i // SLOTS_PER + 1) % TOTAL_NODES for i in range(TOTAL_SLOTS)]

stats = {
    "requests": 0, "reads": 0, "writes": 0,
    "errors": 0, "failovers": 0,
    "degraded_writes": 0, "stale_fallbacks": 0,
    "latencies_ms": [],
}

# ── Routing helpers ───────────────────────────────────────────

def key_to_slot(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % TOTAL_SLOTS


def least_conn(candidates: list) -> int:
    """Return node index with fewest active connections among candidates."""
    best, mn = -1, float("inf")
    for idx in candidates:
        if node_alive[idx] and node_conns[idx] < mn:
            mn, best = node_conns[idx], idx
    return best


def eligible_nodes(slot: int) -> list:
    with lock:
        opts = []
        oi = slot_owner[slot]
        bi = slot_buddy[slot]
        if oi >= 0 and node_alive[oi]:
            opts.append(oi)
        if bi >= 0 and node_alive[bi]:
            opts.append(bi)
        return opts

# ── Routes ────────────────────────────────────────────────────

@app.route("/health")
def health():
    with lock:
        alive = [i for i in range(TOTAL_NODES) if node_alive[i]]
        dead  = [i for i in range(TOTAL_NODES) if not node_alive[i]]
        degraded = [s for s in range(TOTAL_SLOTS) if slot_buddy[s] < 0]
    return jsonify({
        "status": "degraded" if dead else "ok",
        "alive_nodes": alive,
        "dead_nodes": dead,
        "degraded_slots": degraded,
        "total_nodes": TOTAL_NODES,
        "total_slots": TOTAL_SLOTS,
    })


@app.route("/get")
def route_get():
    t0  = time.time()
    key = request.args.get("key", "")

    with lock:
        stats["requests"] += 1
        stats["reads"]    += 1

    slot = key_to_slot(key)
    with lock:
        candidates = eligible_nodes(slot)
        if not candidates:
            stats["errors"] += 1
            return jsonify({"error": "no_live_node", "slot": slot}), 503
        target = least_conn(candidates)
        node_conns[target] += 1
        oi = slot_owner[slot]
        bi = slot_buddy[slot]

    is_primary = (target == oi)
    endpoint   = "/get" if is_primary else "/getsec"

    try:
        r = requests.get(f"{node_url(target)}{endpoint}?slot={slot}", timeout=2)
        resp = r.json()

        # Secondary stale miss → fall back to primary
        if not is_primary and not resp.get("hit") and oi >= 0:
            with lock:
                node_conns[target] -= 1
                if node_alive[oi]:
                    node_conns[oi] += 1
                    fallback = oi
                else:
                    fallback = -1

            if fallback >= 0:
                r = requests.get(f"{node_url(fallback)}/get?slot={slot}", timeout=2)
                resp = r.json()
                with lock:
                    node_conns[fallback] -= 1
                    stats["stale_fallbacks"] += 1
            else:
                with lock:
                    stats["errors"] += 1
                return jsonify({"error": "stale_and_primary_down"}), 503
        else:
            with lock:
                node_conns[target] -= 1

        lat = (time.time() - t0) * 1000
        _record_lat(lat)
        resp["routed_to"] = target
        resp["slot"]      = slot
        resp["key"]       = key
        return jsonify(resp), r.status_code

    except Exception as e:
        with lock:
            node_conns[target] -= 1
            stats["errors"] += 1
        return jsonify({"error": str(e), "key": key}), 502


@app.route("/set", methods=["POST"])
def route_set():
    t0   = time.time()
    data = request.json
    key  = data.get("key", "")
    val  = data.get("value", "")

    with lock:
        stats["requests"] += 1
        stats["writes"]   += 1

    slot = key_to_slot(key)
    with lock:
        oi = slot_owner[slot]
        bi = slot_buddy[slot]
        primary_live = oi >= 0 and node_alive[oi]
        buddy_live   = bi >= 0 and node_alive[bi]

    # Normal path: write to primary (primary async-replicates to buddy via WAL)
    if primary_live:
        with lock:
            node_conns[oi] += 1
        try:
            r = requests.post(f"{node_url(oi)}/set",
                              json={"slot": slot, "value": val}, timeout=2)
            with lock:
                node_conns[oi] -= 1
            lat = (time.time() - t0) * 1000
            _record_lat(lat)
            resp = r.json()
            resp["key"] = key
            return jsonify(resp), r.status_code
        except Exception as e:
            with lock:
                node_conns[oi] -= 1
                stats["errors"] += 1
            return jsonify({"error": str(e)}), 502

    # Degraded path: primary down, write to buddy
    if buddy_live:
        with lock:
            node_conns[bi] += 1
            stats["degraded_writes"] += 1
        try:
            r = requests.post(f"{node_url(bi)}/set",
                              json={"slot": slot, "value": val}, timeout=2)
            with lock:
                node_conns[bi] -= 1
            lat = (time.time() - t0) * 1000
            _record_lat(lat)
            resp = r.json()
            resp["key"] = key
            resp["degraded"] = True
            return jsonify(resp), r.status_code
        except Exception as e:
            with lock:
                node_conns[bi] -= 1
                stats["errors"] += 1
            return jsonify({"error": str(e)}), 502

    with lock:
        stats["errors"] += 1
    return jsonify({"error": "no_live_node_for_slot", "slot": slot}), 503


@app.route("/stats")
def get_stats():
    with lock:
        lats = stats["latencies_ms"]
        sl   = sorted(lats) if lats else [0]
        st   = dict(stats)
        st["p50_ms"] = round(sl[int(len(sl) * 0.50)], 2)
        st["p95_ms"] = round(sl[int(len(sl) * 0.95)], 2)
        st["p99_ms"] = round(sl[min(int(len(sl) * 0.99), len(sl) - 1)], 2)
        st["slot_owner"] = slot_owner[:]
        st["slot_buddy"] = slot_buddy[:]
        st["node_alive"]  = node_alive[:]
        st.pop("latencies_ms", None)
        return jsonify(st)


@app.route("/slot_table")
def slot_table():
    with lock:
        return jsonify({
            "slot_owner": slot_owner[:],
            "slot_buddy": slot_buddy[:],
            "node_alive":  node_alive[:],
            "node_conns":  node_conns[:],
        })


# ── Failover ──────────────────────────────────────────────────

def do_failover(dead_idx: int):
    """
    1. Mark node dead.
    2. Promote buddy's secondaries → primary.
       Update slot_owner[] for those slots.
    3. Mark source node's slots as DEGRADED (buddy=−1).
    """
    with lock:
        if failover_done[dead_idx]:
            return
        failover_done[dead_idx] = True
        node_alive[dead_idx]    = False
        stats["failovers"] += 1

    print(f"[router] FAILOVER: Node{dead_idx} declared dead", flush=True)

    # Slots this node was primary for
    dead_pri = [s for s in range(TOTAL_SLOTS) if slot_owner[s] == dead_idx]
    # Buddy that holds the secondaries
    bud = (dead_idx + 1) % TOTAL_NODES

    if node_alive[bud]:
        try:
            r = requests.post(f"{node_url(bud)}/promote",
                              json={"slots": dead_pri}, timeout=3)
            print(f"[router]   Node{bud} promote → {r.json()}", flush=True)
        except Exception as e:
            print(f"[router]   promote failed: {e}", flush=True)

        with lock:
            for s in dead_pri:
                slot_owner[s] = bud
                slot_buddy[s] = -1   # no new buddy yet → DEGRADED
    else:
        print(f"[router]   !! Buddy Node{bud} also down — DATA LOSS for {dead_pri}",
              flush=True)
        with lock:
            for s in dead_pri:
                slot_owner[s] = -1
                slot_buddy[s] = -1

    # Source node (whose secondaries lived on dead_idx) now has no replica
    src = (dead_idx - 1) % TOTAL_NODES
    with lock:
        for s in range(TOTAL_SLOTS):
            if slot_owner[s] == src:
                slot_buddy[s] = -1
    print(f"[router]   Node{src} now unprotected (secondary was on Node{dead_idx})",
          flush=True)


# ── Heartbeat monitor ─────────────────────────────────────────

def heartbeat_loop():
    while True:
        time.sleep(HB_TICK_MS / 1000)
        for i in range(TOTAL_NODES):
            with lock:
                if failover_done[i]:
                    continue
            try:
                requests.get(f"{node_url(i)}/health", timeout=0.5)
                with lock:
                    last_hb[i] = time.time()
            except Exception:
                with lock:
                    elapsed = (time.time() - last_hb[i]) * 1000
                    if elapsed > HB_TIMEOUT and not failover_done[i]:
                        print(f"[router] HB timeout Node{i} ({elapsed:.0f}ms)",
                              flush=True)
                threading.Thread(target=do_failover, args=(i,), daemon=True).start()

threading.Thread(target=heartbeat_loop, daemon=True).start()


def _record_lat(ms):
    with lock:
        stats["latencies_ms"].append(ms)
        if len(stats["latencies_ms"]) > 2000:
            stats["latencies_ms"].pop(0)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
