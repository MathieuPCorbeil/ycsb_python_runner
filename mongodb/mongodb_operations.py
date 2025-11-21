import json
import os
import subprocess
import time

from halo import Halo


def initialize_mongodb_replica_set(node_count):
    print("Initializing MongoDB replica set...")
    time.sleep(3)

    try:
        members_list = []
        for i in range(node_count):
            priority = 10 if i == 0 else 1
            members_list.append(
                {"_id": i, "host": f"mongo{i + 1}:27017", "priority": priority}
            )

        init_cmd = f'rs.initiate({{_id:"rs0",members:{json.dumps(members_list)}}}, {{force: true}})'

        subprocess.run(
            [
                "sudo",
                "docker",
                "exec",
                "mongo1",
                "mongosh",
                "--eval",
                init_cmd,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )

        max_wait = 120
        start_time = time.time()
        stabilized = False

        spinner = Halo(text="Waiting for replica set to stabilize", spinner="dots")
        spinner.start()

        while time.time() - start_time < max_wait:
            result = subprocess.run(
                [
                    "sudo",
                    "docker",
                    "exec",
                    "mongo1",
                    "mongosh",
                    "--eval",
                    "rs.status().members.map(m => ({_id: m._id, name: m.name, state: m.state, stateStr: m.stateStr}))",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
            )

            if "PRIMARY" in result.stdout or "SECONDARY" in result.stdout:
                stabilized = True
                break

            time.sleep(10)

        if stabilized:
            spinner.succeed(" MongoDB replica set initialized")
        else:
            spinner.warn("MongoDB replica set did not stabilize within timeout")
    except Exception as e:
        print(f"Warning: Could not initialize MongoDB replica set: {e}")


def drop_mongodb_database():
    try:
        subprocess.run(
            [
                "sudo",
                "docker",
                "exec",
                "mongo1",
                "mongosh",
                "--eval",
                "db.getSiblingDB('ycsb').dropDatabase()",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
    except Exception as e:
        print(f"Warning: Could not drop MongoDB database: {e}")


def generate_mongodb_docker_compose(node_count, config):
    db_name = "mongodb"
    docker_compose_path = f"{db_name}/docker-compose-run.yml"

    with open(f"{db_name}/{config['DOCKER_COMPOSE_BASE_FILENAME']}", "r") as f:
        mongodb_yml = f.read()

    for i in range(2, node_count + 1):
        port = 27016 + i
        if port > 27019:
            port = 27121 + i
        mongodb_yml += f"""
  mongo{i}:
    image: mongo:latest
    container_name: mongo{i}
    ports:
      - "{port}:27017"
    command: ["mongod", "--replSet", "rs0", "--port", "27017", "--bind_ip_all"]
    networks:
      - mongo-net
"""

    mongodb_yml += """
networks:
  mongo-net:
    driver: bridge
"""

    with open(docker_compose_path, "w") as f:
        f.write(mongodb_yml)


def handle_mongodb_workload(
    workload_path, params, config, ycsb_wrapper, parse_ycsb_output, save_results_json
):
    results = {
        "workload": os.path.splitext(os.path.basename(params["workload_path"]))[0],
        "database": params["db"],
        "node_count": params["node_count"],
        "phases": [],
    }

    drop_mongodb_database()

    print("Starting YCSB load phase...")
    load_output = ycsb_wrapper(config["YCSB_LOAD_COMMAND"], 0, workload_path)
    load_data = parse_ycsb_output(load_output, "load", 0)
    results["phases"].append(load_data)
    print("\n\n✓ Load phase complete!\n")

    print(f"Starting {params['iteration_count']} run iterations...")
    for i in range(params["iteration_count"]):
        print(
            f"\n Running iteration {i + 1}/{params['iteration_count']}...",
            end="",
            flush=True,
        )
        run_output = ycsb_wrapper(config["YCSB_RUN_COMMAND"], i, workload_path)
        run_data = parse_ycsb_output(run_output, "run", i)
        results["phases"].append(run_data)

    save_results_json(results)
    print("\n✓ Done running all iterations!")
    return results
