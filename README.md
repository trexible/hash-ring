# kv-ring6-k8s

6-Node Ring KV store.

---

## Architecture

```
Client → Router (least-conn, heartbeat, failover)
              ↓
    ┌─────────────────────────────┐
    │   12 slots, 6 nodes         │
    │                             │
    │  kv-0: PRI s0,s1            │
    │  kv-1: PRI s2,s3  SEC s0,s1 │
    │  kv-2: PRI s4,s5  SEC s2,s3 │
    │  kv-3: PRI s6,s7  SEC s4,s5 │
    │  kv-4: PRI s8,s9  SEC s6,s7 │
    │  kv-5: PRI s10,11 SEC s8,s9 │
    └─────────────────────────────┘

    
Write path:  Router → Primary (sync) → WAL (async, 100ms) → Buddy (secondary)
Comment:=> the main bottleneck, if a write occurs, it needs to be pushed to the buddy node, if the the buddy node is also having a write operation pending, it will increase the latency.

Read path:   Router picks least-conn from {primary, buddy}
             If secondary miss → fallback to primary

Failover:    Router heartbeat (300ms tick, 900ms timeout)
             → promote buddy's secondaries → primary
             → mark source node's slots DEGRADED (no replica, until new spawn of the failed node).
```

---

## Prerequisites

- `minikube start --driver=docker`
- `kubectl`
- `python3` with `flask requests` (`pip install flask requests`)

---

## Running

```bash
cd kv-ring6-k8s

# Run all
./run.sh

# Failover for node 2
./run.sh --crash-node2

# Just check pod status
./run.sh --status

# Reset
./run.sh --clean
```

---

## Known Issues (as discussed)

1. **Scaling**: adding a 7th node reshuffles all slot boundaries — same problem as before
2. **Router SPOF**: single router pod, needs active-passive for HA
3. **WAL lag**: secondary may be stale within the 100ms batch window
4. **Degraded mode**: when a node fails, its source node loses its replica with no auto-repair
