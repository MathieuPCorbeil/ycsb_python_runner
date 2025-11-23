import json
import os

from cassandra.cassandra_operations import (
    handle_cassandra_workload as handle_cassandra_workload_impl,
)
from config import CONFIG, params
from mongodb.mongodb_operations import (
    handle_mongodb_workload as handle_mongodb_workload_impl,
)
from redis.redis_operations import (
    handle_redis_workload as handle_redis_workload_impl,
)
from ycsb_handler import parse_ycsb_output, ycsb_wrapper


def prepare_workload(workload_path: str) -> str:
    """Read the workload file and add database-specific connection settings."""
    with open(workload_path, "r") as f:
        workload_data = f.read()

    # Add database-specific configuration
    if params["db"] == "redis":
        workload_data += """
# Redis connection settings (auto-added)
redis.host=localhost
redis.port=6379
"""
    elif params["db"] == "mongodb":
        workload_data += """
# MongoDB connection settings (auto-added)
mongodb.url=mongodb://localhost:27017
"""
    elif params["db"] == "cassandra":
        workload_data += """
# Cassandra connection settings (auto-added)
hosts=localhost
port=9042
"""

    # Write to a temporary workload file with configuration
    output_path = f"{CONFIG['WORKLOADS_PATH']}/{params['db']}_workload_temp.txt"
    with open(output_path, "w") as f:
        f.write(workload_data)

    return output_path


def handle_workload(workload_path: str):
    results = None
    if params["db"] == "redis":
        results = handle_redis_workload_impl(
            workload_path, params, CONFIG, ycsb_wrapper, parse_ycsb_output
        )
        save_results_json(results)
    elif params["db"] == "mongodb":
        results = handle_mongodb_workload_impl(
            workload_path,
            params,
            CONFIG,
            ycsb_wrapper,
            parse_ycsb_output,
            save_results_json,
        )
    elif params["db"] == "cassandra":
        results = handle_cassandra_workload_impl(
            workload_path,
            params,
            CONFIG,
            ycsb_wrapper,
            parse_ycsb_output,
            save_results_json,
        )

    if results is not None:
        # TODO: "Calculate a confidence interval for the averages" (from assignment instructions)

        print("\n✓ Done running all iterations!")
    else:
        print("Error: No results to save. Exiting...")


def cleanup_temp_workload():
    """Delete the temporary workload file created with database settings."""
    temp_workload_path = f"{CONFIG['WORKLOADS_PATH']}/{params['db']}_workload_temp.txt"
    try:
        if os.path.exists(temp_workload_path):
            os.remove(temp_workload_path)
            print(f"✓ Cleaned up temporary workload file: {temp_workload_path}")
    except Exception as e:
        print(f"Warning: Could not delete temporary workload file: {e}")


def save_results_json(results: dict):
    db = params["db"]
    node_count = params["node_count"]
    workload_name = results["workload"]

    results_dir = f"{CONFIG['RESULTS_PATH']}/{db}/{node_count}"
    os.makedirs(results_dir, exist_ok=True)

    results_file = f"{results_dir}/{workload_name}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n\n✓ Results saved to {results_file}")
