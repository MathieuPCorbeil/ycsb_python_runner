"""
HEY,
CONSULT README.md FOR USAGE DETAILS!
"""

import json
import os
import re
import subprocess
import sys

from cassandra.cassandra_operations import (
    generate_cassandra_docker_compose,
    initialize_cassandra_cluster,
)
from cassandra.cassandra_operations import (
    handle_cassandra_workload as handle_cassandra_workload_impl,
)
from mongodb.mongodb_operations import (
    generate_mongodb_docker_compose,
    initialize_mongodb_replica_set,
)
from mongodb.mongodb_operations import (
    handle_mongodb_workload as handle_mongodb_workload_impl,
)
from redis.redis_operations import (
    generate_redis_docker_compose,
)
from redis.redis_operations import (
    handle_redis_workload as handle_redis_workload_impl,
)

# Configuration constants
CONFIG = {
    "YCSB_BIN_PATH": os.path.expanduser("~/ycsb-0.17.0/bin/ycsb.sh"),
    "WORKLOADS_PATH": "./workloads",
    "RESULTS_PATH": "results",
    "DOCKER_COMPOSE_BASE_FILENAME": "docker-compose-base.yml",
    "YCSB_RUN_COMMAND": "run",
    "YCSB_LOAD_COMMAND": "load",
    "SUPPORTED_DBS": ["redis", "mongodb", "cassandra"],
}

# Runtime parameters (set during main())
params = {
    "db": None,
    "node_count": None,
    "workload_path": None,
    "iteration_count": 1,
    "keep_alive": False,
}


def parse_arguments():
    """Parse command-line arguments and validate them."""
    args = sys.argv[1:]

    if "--keep-alive" in args:
        params["keep_alive"] = True
        args.remove("--keep-alive")

    if len(args) < 3:
        print_usage()
        return False

    try:
        params["db"] = validate_db(args[0])
        params["node_count"] = validate_node_count(int(args[1]))
        params["workload_path"] = validate_workload_path(args[2])
        if len(args) > 3:
            params["iteration_count"] = validate_iteration_count(int(args[3]))
        cleanup_containers()
        return True
    except ValueError as e:
        print(f"Error: {e}")
        return False


def print_usage():
    """Print usage information."""
    print(
        "Usage: python script.py <db> <node_count> <workload_file> [iterations] [--keep-alive]"
    )
    print(f"  db: {' or '.join(CONFIG['SUPPORTED_DBS'])}")
    print("  node_count: positive integer")
    print("  workload_file: path to workload file in ./workloads/")
    print("  iterations: number of run iterations (optional, default: 1)")
    print("  --keep-alive: keep containers running after exit")
    print("\nNote: Read/write ratios are defined in the workload file itself")


def validate_db(db):
    """Validate and return database name."""
    if db not in CONFIG["SUPPORTED_DBS"]:
        raise ValueError(
            f"Invalid db. Please use either {' or '.join(CONFIG['SUPPORTED_DBS'])}"
        )
    return db


def validate_node_count(count):
    """Validate and return node count."""
    if count <= 0:
        raise ValueError("Invalid node count. Please use a positive integer")
    return count


def validate_iteration_count(count):
    """Validate and return iteration count."""
    if count <= 0:
        raise ValueError("Invalid iteration count. Please use a positive integer")
    return count


def cleanup_containers():
    """Clean up Docker containers."""
    print("Cleaning up containers...")
    try:
        subprocess.run(
            [
                "sudo",
                "docker",
                "compose",
                "-f",
                f"{params['db']}/docker-compose-run.yml",
                "down",
                "--remove-orphans",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=True,  # Raise exception if non-zero exit
        )
        print("✓ Containers cleaned up")
    except Exception as e:
        print(f"Warning: Could not clean up all containers: {e}")


def validate_workload_path(workload_file):
    """Validate and return the full workload path."""
    workload_path = f"{CONFIG['WORKLOADS_PATH']}/{workload_file}"
    if not os.path.exists(workload_path):
        raise ValueError(f"Workload file does not exist: {workload_path}")
    return workload_path


def main():
    """Main entry point."""
    if not parse_arguments():
        return 1

    print(
        f"Setting up Docker containers for {params['db'].upper()} with {params['node_count']} nodes..."
    )
    generate_docker_compose()
    run_docker_compose()
    print("✓ Docker setup complete!\n")

    # Create a workload file with database connection settings
    workload_with_config = prepare_workload(params["workload_path"])

    print("Starting YCSB workload with provided workload file...")
    handle_workload(workload_with_config)

    return 0


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


def generate_docker_compose():
    if params["db"] == "redis":
        generate_redis_docker_compose(params["node_count"], CONFIG)
    elif params["db"] == "mongodb":
        generate_mongodb_docker_compose(params["node_count"], CONFIG)
    elif params["db"] == "cassandra":
        generate_cassandra_docker_compose(params["node_count"], CONFIG)


def run_docker_compose():
    db_name = params["db"]
    subprocess.run(
        [
            "sudo",
            "docker",
            "compose",
            "-f",
            f"{db_name}/docker-compose-run.yml",
            "up",
            "-d",
        ]
    )

    if db_name == "mongodb":
        initialize_mongodb_replica_set(params["node_count"])
    elif db_name == "cassandra":
        initialize_cassandra_cluster(params["node_count"])


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
        print("\n✓ Done running all iterations!")
    else:
        print("Error: No results to save. Exiting...")


def ycsb_wrapper(command_type: str, iteration: int, workload_path: str) -> str:
    db = params["db"]
    db_binding = "cassandra-cql" if db == "cassandra" else db
    output_lines = []

    try:
        process = subprocess.Popen(
            [
                CONFIG["YCSB_BIN_PATH"],
                command_type,
                db_binding,
                "-s",
                "-P",
                workload_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )

        cmd = [
            CONFIG["YCSB_BIN_PATH"],
            command_type,
            db_binding,
            "-s",
            "-P",
            workload_path,
        ]
        print(f" Running: {' '.join(cmd)}")

        try:
            stdout_data, stderr_data = process.communicate(timeout=600)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout_data, stderr_data = process.communicate()
            print("\n    ERROR: YCSB command timed out after 10 minutes")

        for line in stdout_data.split("\n"):
            if line:
                output_lines.append(line)
                if (
                    "[READ]" in line
                    or "[UPDATE]" in line
                    or "[OVERALL]" in line
                    or "[INSERT]" in line
                ):
                    print(f"\n    {line.strip()}", end="")

        if process.returncode != 0:
            print(f"\n    WARNING: YCSB exited with code {process.returncode}")

    except Exception as e:
        print(f"\n    ERROR: {str(e)}")

    return "\n".join(output_lines)


def parse_ycsb_output(output: str, phase: str, iteration: int) -> dict:
    phase_data = {
        "phase": phase,
        "iteration": iteration,
        "overall": {},
        "operations": {},
    }

    lines = output.split("\n")
    for line in lines:
        line = line.strip()

        if line.startswith("[OVERALL], RunTime(ms),"):
            runtime = float(line.split(",")[2])
            phase_data["overall"]["runtime_ms"] = runtime

        elif line.startswith("[OVERALL], Throughput(ops/sec),"):
            throughput = float(line.split(",")[2])
            phase_data["overall"]["throughput_ops_sec"] = throughput

        elif re.match(r"\[(READ|INSERT|UPDATE|DELETE|CLEANUP)\], Operations,", line):
            op_type = re.match(r"\[(\w+)\]", line).group(1)
            count = int(line.split(",")[2])
            if op_type not in phase_data["operations"]:
                phase_data["operations"][op_type] = {}
            phase_data["operations"][op_type]["count"] = count

        elif re.match(
            r"\[(READ|INSERT|UPDATE|DELETE|CLEANUP)\], AverageLatency\(us\),", line
        ):
            op_type = re.match(r"\[(\w+)\]", line).group(1)
            latency = float(line.split(",")[2])
            if op_type not in phase_data["operations"]:
                phase_data["operations"][op_type] = {}
            phase_data["operations"][op_type]["avg_latency_us"] = latency

        elif re.match(
            r"\[(READ|INSERT|UPDATE|DELETE|CLEANUP)\], MinLatency\(us\),", line
        ):
            op_type = re.match(r"\[(\w+)\]", line).group(1)
            latency = float(line.split(",")[2])
            if op_type not in phase_data["operations"]:
                phase_data["operations"][op_type] = {}
            phase_data["operations"][op_type]["min_latency_us"] = latency

        elif re.match(
            r"\[(READ|INSERT|UPDATE|DELETE|CLEANUP)\], MaxLatency\(us\),", line
        ):
            op_type = re.match(r"\[(\w+)\]", line).group(1)
            latency = float(line.split(",")[2])
            if op_type not in phase_data["operations"]:
                phase_data["operations"][op_type] = {}
            phase_data["operations"][op_type]["max_latency_us"] = latency

        elif re.match(
            r"\[(READ|INSERT|UPDATE|DELETE|CLEANUP)\], 95thPercentileLatency\(us\),",
            line,
        ):
            op_type = re.match(r"\[(\w+)\]", line).group(1)
            latency = float(line.split(",")[2])
            if op_type not in phase_data["operations"]:
                phase_data["operations"][op_type] = {}
            phase_data["operations"][op_type]["p95_latency_us"] = latency

        elif re.match(
            r"\[(READ|INSERT|UPDATE|DELETE|CLEANUP)\], 99thPercentileLatency\(us\),",
            line,
        ):
            op_type = re.match(r"\[(\w+)\]", line).group(1)
            latency = float(line.split(",")[2])
            if op_type not in phase_data["operations"]:
                phase_data["operations"][op_type] = {}
            phase_data["operations"][op_type]["p99_latency_us"] = latency

        elif re.match(r"\[(READ|INSERT|UPDATE|DELETE)\], Return=OK,", line):
            op_type = re.match(r"\[(\w+)\]", line).group(1)
            count = int(line.split(",")[2])
            if op_type not in phase_data["operations"]:
                phase_data["operations"][op_type] = {}
            phase_data["operations"][op_type]["return_ok"] = count

    return phase_data


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


def cleanup_temp_workload():
    """Delete the temporary workload file created with database settings."""
    temp_workload_path = f"{CONFIG['WORKLOADS_PATH']}/{params['db']}_workload_temp.txt"
    try:
        if os.path.exists(temp_workload_path):
            os.remove(temp_workload_path)
            print(f"✓ Cleaned up temporary workload file: {temp_workload_path}")
    except Exception as e:
        print(f"Warning: Could not delete temporary workload file: {e}")


if __name__ == "__main__":
    try:
        exit_code = main()
    finally:
        cleanup_temp_workload()
        if params["keep_alive"]:
            print("Containers will remain running (--keep-alive).")
        else:
            cleanup_containers()
    sys.exit(exit_code)
