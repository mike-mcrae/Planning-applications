#!/usr/bin/env bash

N_WORKERS=10
SCRIPT="0.scrape.py"

cd "$(dirname "$0")" || exit 1

for ((i=0; i<${N_WORKERS}; i++)); do
    SESSION="obs_worker_${i}"

    echo "Launching ${SESSION}"

    screen -dmS "${SESSION}" \
        bash -c "python3 ${SCRIPT} ${i} ${N_WORKERS}; exec bash"
done

echo
echo "Launched ${N_WORKERS} screen sessions."
echo
echo "Attach with:  screen -r obs_worker_0"
echo "Detach with:  Ctrl-A D"
echo "List with:    screen -ls"
