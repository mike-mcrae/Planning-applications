#!/usr/bin/env bash

echo "Killing all obs_worker_* screen sessions..."

screen -ls | awk '/obs_worker_[0-9]+/ {print $1}' | while read -r session; do
    echo "  → killing $session"
    screen -S "$session" -X quit
done

echo "Done."
