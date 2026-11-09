#!/bin/sh
# Crash, snapshot catch-up, failover and partition against a real container cluster.
# Uses its own Compose project and removes only that project's containers and volumes.
set -eu
cd "$(dirname -- "$0")/.."
export COMPOSE_PROJECT_NAME=kvstore-demo KV_SNAPSHOT_ENTRIES=50 KV_SNAPSHOT_RETAIN=5
members=1=node1:50051,2=node2:50051,3=node3:50051

compose() { docker compose "$@"; }
tool() { compose exec -T client "$@"; }
kv() { tool kvclient "$members" --timeout-ms 5000 "$@"; }
status() { tool kvadmin "$1:50051" status 2>/dev/null || true; }
field() { printf '%s\n' "$1" | tr ' ' '\n' | sed -n "s/^$2=//p"; }
step() { printf '\n=== %s\n' "$1"; }
fail() { echo "DEMO FAILED: $1" >&2; compose logs --tail 20 >&2 || true; exit 1; }
pause() { perl -e 'select(undef,undef,undef,$ARGV[0])' "$1"; }
cleanup() { compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

# Print the node reporting role=LEADER, skipping the node named in $1.
leader() {
    attempt=0
    while [ "$attempt" -lt 60 ]; do
        for node in node1 node2 node3; do
            [ "$node" = "${1:-}" ] && continue
            case "$(status "$node")" in *role=LEADER*) echo "$node"; return 0 ;; esac
        done
        attempt=$((attempt + 1)); pause 0.25
    done
    return 1
}
# Wait until a node's local copy of key $2 equals $3.
wait_value() {
    attempt=0
    while [ "$attempt" -lt 120 ]; do
        [ "$(tool kvadmin "$1:50051" local-get "$2" 2>/dev/null || true)" = "$3" ] && return 0
        attempt=$((attempt + 1)); pause 0.25
    done
    return 1
}
others() { for node in node1 node2 node3; do [ "$node" = "$1" ] || echo "$node"; done; }

cleanup
step "Build the image and start three containers"
compose up -d --build --wait node1 node2 node3
compose up -d client

current=$(leader) || fail "no leader elected"
echo "leader: $current"
status node1; status node2; status node3

step "Write and read through the client"
kv put name Alice
[ "$(kv get name)" = Alice ] || fail "read after write"
echo "name = Alice"

step "Kill a follower, then write past the snapshot threshold"
victim=$(others "$current" | head -n 1)
compose kill "$victim" >/dev/null
echo "killed: $victim"
i=0
while [ "$i" -lt 120 ]; do kv put "key$((i % 10))" "value$i" >/dev/null || fail "write $i"; i=$((i + 1)); done
kv put name Bob >/dev/null
status "$current"
base=$(field "$(status "$current")" base)
[ "${base:-0}" -gt 0 ] || fail "leader did not truncate its log"
echo "leader truncated its log through index $base"

step "Restart the follower; it must catch up from a snapshot"
compose start "$victim" >/dev/null
wait_value "$victim" name Bob || fail "$victim did not catch up"
status "$victim"
follower_base=$(field "$(status "$victim")" base)
[ "${follower_base:-0}" -gt 0 ] || fail "$victim did not install a snapshot"
echo "$victim: name = $(tool kvadmin "$victim:50051" local-get name)"

step "Kill the leader; the survivors elect a new one"
old=$current
compose kill "$old" >/dev/null
current=$(leader "$old") || fail "no new leader"
echo "old leader $old killed, new leader: $current"
kv put after failover >/dev/null || fail "write after failover"
compose start "$old" >/dev/null
wait_value "$old" after failover || fail "$old did not rejoin"
echo "$old rejoined: after = $(tool kvadmin "$old:50051" local-get after)"

step "Partition the leader away from the network"
current=$(leader) || fail "no leader before partition"
network=${COMPOSE_PROJECT_NAME}_kvnet
container=$(compose ps -q "$current")
docker network disconnect "$network" "$container"
echo "disconnected: $current"
peer=$(others "$current" | head -n 1)
attempt=0
while [ "$attempt" -lt 60 ]; do
    if kv put partitioned majority >/dev/null 2>&1; then break; fi
    attempt=$((attempt + 1)); pause 0.25
done
[ "$attempt" -lt 60 ] || fail "majority did not accept a write"
[ "$(tool kvadmin "$peer:50051" local-get partitioned)" = majority ] || wait_value "$peer" partitioned majority || fail "peer missing write"
stale=$(compose exec -T "$current" kvadmin 127.0.0.1:50051 local-get partitioned 2>/dev/null || true)
[ "$stale" != majority ] || fail "isolated node should not have the majority write"
echo "majority accepted the write; isolated $current does not have it"
docker network connect --alias "$current" "$network" "$container"
wait_value "$current" partitioned majority || fail "$current did not heal"
echo "healed: $current = $(tool kvadmin "$current:50051" local-get partitioned)"

step "Full restart keeps committed data"
compose restart node1 node2 node3 >/dev/null
leader >/dev/null || fail "no leader after restart"
[ "$(kv get name)" = Bob ] || fail "name lost after restart"
[ "$(kv get partitioned)" = majority ] || fail "partitioned write lost after restart"
echo "name = Bob, partitioned = majority"

step "All demo steps passed"
