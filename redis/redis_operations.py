import os


def generate_redis_docker_compose(node_count, config):
    db_name = "redis"
    docker_compose_path = f"{db_name}/docker-compose-run.yml"

    with open(f"{db_name}/{config['DOCKER_COMPOSE_BASE_FILENAME']}", "r") as f:
        redis_yml = f.read()

    for i in range(1, node_count):
        redis_yml += f"""
  redis-replica-{i}:
    image: redis:latest
    networks:
      - redis-net
    command: redis-server --appendonly yes --slaveof redis-master 6379
"""

    redis_yml += f"""
networks:
  redis-net:
    driver: bridge
"""

    with open(docker_compose_path, "w") as f:
        f.write(redis_yml)


def handle_redis_workload(
    workload_path, params, config, ycsb_wrapper, parse_ycsb_output
):
    results = {
        "workload": os.path.splitext(os.path.basename(params["workload_path"]))[0],
        "database": params["db"],
        "node_count": params["node_count"],
        "phases": [],
    }

    print("Starting YCSB load phase...")
    load_output = ycsb_wrapper(config["YCSB_LOAD_COMMAND"], 0, workload_path)
    load_data = parse_ycsb_output(load_output, "load", 0)
    results["phases"].append(load_data)
    print("\n\n✓ Load phase complete!\n")
    print(f"Starting {params['iteration_count']} run iterations...")
    for i in range(params["iteration_count"]):
        print(
            f"\n Running iteration {i+1}/{params['iteration_count']}...",
            end="",
            flush=True,
        )
        run_output = ycsb_wrapper(config["YCSB_RUN_COMMAND"], i, workload_path)
        run_data = parse_ycsb_output(run_output, "run", i)
        results["phases"].append(run_data)

    return results
