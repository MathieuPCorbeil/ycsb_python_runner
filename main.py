"""
HEY,
CONSULT README.md FOR USAGE DETAILS!
"""

import sys

from config import CONFIG, params
from docker_handler import (
    cleanup_containers,
    generate_docker_compose,
    run_docker_compose,
)
from utils import (
    validate_db,
    validate_iteration_count,
    validate_node_count,
    validate_workload_path,
)
from workload_handler import cleanup_temp_workload, handle_workload, prepare_workload


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
