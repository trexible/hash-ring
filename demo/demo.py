#!/usr/bin/env python3
"""
3-Phase Ring KV Demo
  Phase 1 — Read from all 6 nodes (direct + via router)
  Phase 2 — Write a key, watch WAL sync to buddy in real-time
  Phase 3 — Kill pod kv-2, buddy serves traffic, wait for respawn
"""
import argparse, json, time, sys, hashlib, subprocess, os, signal
from urllib.request import urlopen, Request
from urllib.error   import URLError

# ── ANSI ──────────────────────────────────────────────────────
CYAN  = "\033[0;36m";  GREEN  = "\033[0;32m"
YEL   = "\033[1;33m";  RED    = "\033[0;31m"
BOLD  = "\033[1m";     DIM    = "\033[2m";  NC = "\033[0m"
TICK  = f"{GREEN}✓{NC}";  CROSS = f"{RED}✗{NC}";  ARR = f"{YEL}→{NC}"

def hdr(n, title):
    bar = "═" * max(2, 54 - len(title))
    print(f"\n{CYAN}{BOLD}╔══ Phase {n}: {title} {bar}╗{NC}")

def sub(s):  print(f"\n{BOLD}  ┌─ {s}{NC}")
def ok(s):   print(f"  {TICK}  {s}")
def err(s):  print(f"  {CROSS}  {RED}{s}{NC}")
def inf(s):  print(f"  {ARR}  {s}")
def dim(s):  print(f"  {DIM}{s}{NC}")
def pause(msg="Press Enter to continue…"):
    print(f"\n  {DIM}{msg}{NC}"); input()

# ── HTTP helpers ───────────────────────────────────────────────
def _get(url, timeout=3):
    with urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())

def _post(url, payload, timeout=3):
    data = json.dumps(payload).encode()
    req  = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def get(base, path):            return _get(f"{base}{path}")
def post(base, path, body):     return _post(f"{base}{path}", body)
def safe_get(base, path):
    try:    return get(base, path)
    except: return None

# ── Topology helpers ───────────────────────────────────────────
def key_slot(k):             return int(hashlib.md5(k.encode()).hexdigest(), 16) % 12
def node_primary_slots(i):   return [2*i, 2*i+1]
def slot_primary_node(s):    return s // 2
def slot_buddy_node(s):      return (slot_primary_node(s) + 1) % 6

# One key guaranteed to hash to each slot
SLOT_KEY = {
    0:"demo_1",  1:"demo_13", 2:"demo_3",  3:"demo_24",
    4:"demo_16", 5:"demo_2",  6:"demo_8",  7:"demo_15",
    8:"demo_21", 9:"demo_10", 10:"demo_5", 11:"demo_0",
}

def node_label(d):
    """Extract node label from a response dict (pod or node_id field)."""
    pod = d.get("pod", "")
    if pod:
        return pod.replace("kv-", "Node")
    nid = d.get("node_id")
    return f"Node{nid}" if nid is not None else "?"

# ─────────────────────────────────────────────────────────────
# PHASE 1 — Read from all 6 nodes
# ─────────────────────────────────────────────────────────────
def phase1_reads(router, node_urls):
    hdr(1, "Read from All 6 Nodes")

    sub("Writing one key per node via router")
    for node_id in range(6):
        for slot in node_primary_slots(node_id):
            key = SLOT_KEY[slot]
            val = f"node{node_id}_slot{slot}_v1"
            try:
                d = post(router, "/set", {"key": key, "value": val})
                ok(f"WRITE  key={key!r:12s}  slot={slot}  "
                   f"→ {node_label(d)}  ver={d.get('ver','?')}")
            except Exception as e:
                err(f"WRITE failed for {key!r}: {e}")

    pause("Keys written. Press Enter to read them back from each node directly…")

    sub("Reading directly from each node (bypassing router)")
    all_ok = True
    for node_id in range(6):
        url = node_urls[node_id]
        readings = []
        for slot in node_primary_slots(node_id):
            d = safe_get(url, f"/get?slot={slot}")
            if d and d.get("hit"):
                readings.append(f"slot{slot}={d['value']!r}(v{d['ver']})")
            else:
                readings.append(f"slot{slot}=MISS")
                all_ok = False
        h = safe_get(url, "/health")
        state = "UP" if h else "DOWN"
        lbl   = ok if state == "UP" else err
        lbl(f"Node{node_id}  [{state}]  pri_slots={node_primary_slots(node_id)}  "
            f"pri_keys={h['pri_keys'] if h else '?'}  │  {',  '.join(readings)}")

    print()
    if all_ok:
        ok(f"{GREEN}All 6 nodes serving their primary slots correctly.{NC}")
    else:
        err("Some nodes missing data — check port-forwards.")

    pause("Phase 1 done. Press Enter to watch WAL sync…")

# ─────────────────────────────────────────────────────────────
# PHASE 2 — Write + buddy WAL sync (real-time polling)
# ─────────────────────────────────────────────────────────────
def phase2_wal(router, node_urls):
    hdr(2, "Write → WAL → Buddy Sync")

    demo_slot   = 4
    demo_key    = SLOT_KEY[demo_slot]
    primary_id  = slot_primary_node(demo_slot)   # 2
    buddy_id    = slot_buddy_node(demo_slot)      # 3
    primary_url = node_urls[primary_id]
    buddy_url   = node_urls[buddy_id]

    inf(f"slot {demo_slot}  →  primary=Node{primary_id}(kv-{primary_id})  "
        f"buddy=Node{buddy_id}(kv-{buddy_id})")
    inf(f"key={demo_key!r}  hashes to slot {key_slot(demo_key)}")

    sub("Step 1 — snapshot buddy BEFORE write")
    before = safe_get(buddy_url, f"/getsec?slot={demo_slot}")
    if before and before.get("hit"):
        inf(f"Buddy Node{buddy_id} has:  ver={before['ver']}  val={before['value']!r}  "
            f"← baseline")
    else:
        ok(f"Buddy Node{buddy_id} has nothing for slot {demo_slot}  (clean slate)")

    pause("Press Enter to fire the write and watch the buddy live…")

    sub("Step 2 — write via router")
    new_val = f"wal_demo_v{int(time.time())}"
    t_write = time.time()
    try:
        d = post(router, "/set", {"key": demo_key, "value": new_val})
        t_ack = time.time()
        ok(f"WRITE ack  key={demo_key!r}  val={new_val!r}  ver={d.get('ver')}  "
           f"→ {node_label(d)}  ({(t_ack-t_write)*1000:.1f}ms)")
    except Exception as e:
        err(f"Write failed: {e}"); return

    new_ver = d.get("ver", 0)

    sub("Step 3 — polling buddy every 20ms until WAL flushes")
    inf("(WAL_FLUSH_MS=100 — flush happens in the background)")
    print()
    found_at_ms = None
    for tick in range(25):      # up to 500ms
        elapsed = (time.time() - t_write) * 1000
        b = safe_get(buddy_url, f"/getsec?slot={demo_slot}")
        hit     = b and b.get("hit")
        cur_ver = b.get("ver", 0) if b else 0
        cur_val = b.get("value", "") if hit else "—"
        synced  = hit and cur_ver >= new_ver

        bar   = f"{GREEN}●{NC}" if synced else f"{YEL}○{NC}"
        state = (f"{GREEN}SYNCED{NC}  ver={cur_ver}" if synced
                 else f"{DIM}stale   ver={cur_ver}{NC}")
        print(f"\r    {bar}  +{elapsed:5.0f}ms  buddy val={cur_val!r:28s}  {state}    ",
              end="", flush=True)

        if synced:
            found_at_ms = elapsed
            break
        time.sleep(0.02)

    print()
    if found_at_ms is not None:
        ok(f"WAL flushed to buddy in {found_at_ms:.0f}ms after primary ack  ✓")
    else:
        err("Buddy didn't sync within 500ms — increase polling window?")

    sub("Step 4 — verify primary hasn't changed")
    d = safe_get(primary_url, f"/get?slot={demo_slot}")
    if d and d.get("hit"):
        ok(f"Primary Node{primary_id}:  ver={d['ver']}  val={d['value']!r}  "
           f"({d['latency_ms']}ms)")

    pause("Phase 2 done. Press Enter to run the failover demo…")

# ─────────────────────────────────────────────────────────────
# PHASE 3 — Pod kv-2 crash → buddy takeover → respawn
# ─────────────────────────────────────────────────────────────
def phase3_failover(router, node_urls, node2_local_port):
    hdr(3, "Pod kv-2 Crashes → Buddy Takeover → Respawn")

    sub("Before crash — slot table")
    _print_slot_table(router)

    sub("Before crash — all nodes healthy")
    for i, url in enumerate(node_urls):
        h = safe_get(url, "/health")
        s = "UP" if h else "DOWN"
        (ok if s == "UP" else err)(
            f"Node{i}(kv-{i})  {s}  "
            f"pri_slots={node_primary_slots(i)}  "
            f"pri_keys={h['pri_keys'] if h else '?'}  "
            f"sec_keys={h['sec_keys'] if h else '?'}")

    pause("Press Enter to delete pod kv-2 (force-kill, no grace period)…")

    sub("Killing kv-2")
    inf("kubectl delete pod kv-2 --grace-period=0 --force")
    try:
        subprocess.run(
            ["kubectl", "delete", "pod", "kv-2",
             "--grace-period=0", "--force"],
            check=True, capture_output=True
        )
        ok("Pod kv-2 deleted")
    except subprocess.CalledProcessError as e:
        err(f"kubectl failed: {e.stderr.decode().strip()}"); return
    except FileNotFoundError:
        err("kubectl not found — run from the minikube environment"); return

    sub("Waiting for router heartbeat timeout (~900ms)")
    for i in range(18):
        time.sleep(0.1)
        print(f"\r    {DIM}…{(i+1)*100}ms{NC}", end="", flush=True)
    print()
    time.sleep(0.3)

    sub("Slot table after failover")
    _print_slot_table(router)

    sub("Reads on kv-2's slots (4,5) — should be served by Node3 (buddy)")
    for slot in [4, 5]:
        key = SLOT_KEY[slot]
        try:
            d = get(router, f"/get?key={key}")
            if d.get("hit"):
                ok(f"READ slot={slot}  key={key!r}  val={d.get('value')!r}  "
                   f"→ {node_label(d)}  ver={d.get('ver')}  "
                   f"← {GREEN}buddy serving{NC}")
            else:
                err(f"READ slot={slot}  MISS")
        except Exception as e:
            err(f"Read slot={slot} failed: {e}")

    sub("Reads on kv-1's slots (2,3) — buddy was kv-2, now DEGRADED (single copy)")
    for slot in [2, 3]:
        key = SLOT_KEY[slot]
        try:
            d = get(router, f"/get?key={key}")
            if d.get("hit"):
                ok(f"READ slot={slot}  key={key!r}  → {node_label(d)}  "
                   f"ver={d.get('ver')}  {YEL}⚠ single copy, no buddy{NC}")
        except Exception as e:
            inf(f"slot {slot} not readable: {e}")

    pause("Press Enter to wait for StatefulSet to respawn kv-2…")

    sub("Waiting for kv-2 pod to appear in Running state")
    inf("Polling: kubectl get pod kv-2 every 2s…")
    for attempt in range(30):
        time.sleep(2)
        result = subprocess.run(
            ["kubectl", "get", "pod", "kv-2",
             "--no-headers", "-o", "custom-columns=STATUS:.status.phase"],
            capture_output=True, text=True
        )
        status = result.stdout.strip()
        print(f"\r    {DIM}…{(attempt+1)*2}s  pod status: {status or 'Pending'}{NC}   ",
              end="", flush=True)
        if "Running" in status:
            print()
            ok(f"Pod kv-2 is Running  (attempt {attempt+1})")
            break
    else:
        print()
        err("kv-2 didn't reach Running in 60s — check: kubectl get pods"); return

    # Port-forward is stale — restart it for the new pod
    sub("Restarting port-forward to new kv-2 pod")
    inf(f"Old port-forward (to dead pod) is stale — killing and restarting on :{node2_local_port}")
    # Kill anything holding the port
    subprocess.run(
        f"lsof -ti tcp:{node2_local_port} | xargs kill -9 2>/dev/null || true",
        shell=True
    )
    time.sleep(0.5)
    fwd = subprocess.Popen(
        ["kubectl", "port-forward", f"pod/kv-2",
         f"{node2_local_port}:8080"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(2)   # let port-forward stabilise

    # Now poll via the fresh port-forward
    node2_url = f"http://localhost:{node2_local_port}"
    for attempt in range(10):
        h = safe_get(node2_url, "/health")
        if h and h.get("status") == "ok":
            ok(f"kv-2 responding via new port-forward")
            break
        time.sleep(1)
    else:
        err("kv-2 port-forward didn't stabilise — check manually")
        return

    sub("Post-respawn state")
    _print_slot_table(router)

    for i, url in enumerate(node_urls):
        h = safe_get(url, "/health")
        s = "UP" if h else "DOWN"
        note = ""
        if i == 2 and h:
            note = (f"  {YEL}← fresh pod  "
                    f"pri_keys={h['pri_keys']} (empty — needs recovery){NC}")
        (ok if s == "UP" else err)(
            f"Node{i}  {s}  "
            f"pri_keys={h['pri_keys'] if h else '?'}  "
            f"sec_keys={h['sec_keys'] if h else '?'}"
            f"{note}")

    print()
    inf(f"{YEL}Open problem:{NC} kv-2 is back but its in-memory store is empty.")
    inf("The StatefulSet respawns the pod — not the data.")
    inf("Recovery options: replay WAL from kv-3 (buddy), or use a PersistentVolume.")

    # Clean up our extra port-forward
    fwd.terminate()

# ─────────────────────────────────────────────────────────────
# Slot table printer
# ─────────────────────────────────────────────────────────────
def _print_slot_table(router):
    try:
        d       = get(router, "/slot_table")
        owners  = d["slot_owner"]
        buddies = d["slot_buddy"]
        alive   = d.get("node_alive", [True]*6)
        print(f"\n  {'Slot':<6}{'Owner':<14}{'Buddy':<14}{'Status'}")
        print(f"  {'─'*6}{'─'*14}{'─'*14}{'─'*16}")
        for s in range(12):
            oi, bi = owners[s], buddies[s]
            on  = (f"{RED}Node{oi}(dead){NC}" if oi >= 0 and not alive[oi]
                   else f"Node{oi}" if oi >= 0 else "NONE")
            bn  = (f"{RED}NONE{NC}" if bi < 0
                   else f"Node{bi}")
            st  = (f"{GREEN}OK{NC}" if bi >= 0
                   else f"{RED}⚠ DEGRADED{NC}")
            print(f"  {s:<6}{on:<14}{bn:<14}{st}")
        print()
    except Exception as e:
        err(f"Could not fetch slot table: {e}")

# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--router",         default="http://localhost:9000")
    p.add_argument("--nodes",          default=",".join(
        [f"http://localhost:{9001+i}" for i in range(6)]))
    p.add_argument("--node2",          default="http://localhost:9003")
    p.add_argument("--node2-port",     type=int, default=9003,
                   help="Local port for kv-2 port-forward (used to restart it after respawn)")
    p.add_argument("--skip-pause",     action="store_true")
    args = p.parse_args()

    router    = args.router.rstrip("/")
    node_urls = [u.strip() for u in args.nodes.split(",")]

    if args.skip_pause:
        import builtins
        builtins.input = lambda *_: None

    try:
        h = get(router, "/health")
        alive = h.get("alive_nodes", [])
        print(f"\n  {TICK} Router reachable — {len(alive)} nodes alive: {alive}")
    except Exception as e:
        err(f"Router unreachable at {router}: {e}")
        sys.exit(1)

    phase1_reads(router, node_urls)
    phase2_wal(router, node_urls)
    phase3_failover(router, node_urls, args.node2_port)

    print(f"\n{CYAN}{BOLD}╔══ Done ══════════════════════════════════════════════════╗{NC}")
    print(f"  All three phases complete.\n")

if __name__ == "__main__":
    main()
