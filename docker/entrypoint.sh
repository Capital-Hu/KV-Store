#!/bin/sh
# Decide new/existing from the data itself so a restarted container keeps its identity.
# A half-created directory is deliberately not treated as new: kvnode refuses it.
set -eu
: "${KV_NODE_ID:?KV_NODE_ID is required}"
: "${KV_MEMBERS:?KV_MEMBERS is required}"
directory=/data/node
mode=new
if [ -e "$directory" ]; then mode=existing; fi
exec kvnode --id "$KV_NODE_ID" --directory "$directory" --mode "$mode" --members "$KV_MEMBERS" \
    --listen "0.0.0.0:${KV_PORT:-50051}" \
    --snapshot-entries "${KV_SNAPSHOT_ENTRIES:-100000}" --snapshot-retain "${KV_SNAPSHOT_RETAIN:-1000}"
