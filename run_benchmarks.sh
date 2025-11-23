#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# YCSB Benchmarking Automation Script. 
# Adjust parameters as needed.
DATABASES=("redis" "mongodb" "cassandra")
NODE_COUNTS=(3 5)
# TODO: Based on instructions, we need to choose at least 3 workloads. I chose A, B and E for now. Gotta decide in team.
WORKLOADS=("workloada" "workloadb" "workloade") 
ITERATIONS=10

for DB in "${DATABASES[@]}"; do
    for NODES in "${NODE_COUNTS[@]}"; do
        for WORKLOAD in "${WORKLOADS[@]}"; do
            echo "⚙️  Running $DB with $NODES nodes, workload $WORKLOAD, $ITERATIONS iterations..."
            python3 main.py "$DB" "$NODES" "$WORKLOAD" "$ITERATIONS"
            echo "Finished $DB with $NODES nodes, workload $WORKLOAD"
            echo "---------------------------------------------"
        done
    done
done

echo "⚙️  All benchmarks completed!"
