#!/usr/bin/env bash

N_WORKERS=10

# FULL ABSOLUTE PATH (quoted)
SCRIPT="/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/5d.run_parrallel.py"

for ((i=0; i<${N_WORKERS}; i++)); do
    SESSION="geo_worker_${i}"

    echo "Launching ${SESSION}"

    screen -dmS "${SESSION}" bash -c "
        python3 \"${SCRIPT}\" ${i} ${N_WORKERS};
        exec bash
    "
done

echo
echo "Launched ${N_WORKERS} workers."
echo "Check with: screen -ls"
