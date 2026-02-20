#!/usr/bin/env bash
set -e

N_WORKERS=2
SCRIPT="3f.rerun_remaining_download_failures.py"

cd "$(dirname "$0")" || exit 1

for ((i=0; i<${N_WORKERS}; i++)); do
    SESSION="round3_worker_${i}"
    LOGFILE="round3_worker_${i}.log"

    echo "Launching ${SESSION}"

    screen -dmS "${SESSION}" bash -c "
        echo 'Starting worker ${i}';
        python3 ${SCRIPT} ${i} ${N_WORKERS} > ${LOGFILE} 2>&1;
        EXIT_CODE=\$?;
        echo 'Worker ${i} exited with code' \$EXIT_CODE;
        exit \$EXIT_CODE
    "
done

echo
echo "Launched ${N_WORKERS} screen sessions."
echo
echo "List with:    screen -ls"
echo "Logs:         round3_worker_0.log, round3_worker_1.log"
