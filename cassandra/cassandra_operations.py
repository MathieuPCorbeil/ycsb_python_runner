import os
import subprocess
import time

from halo import Halo


def initialize_cassandra_cluster(node_count):
    print("Initializing Cassandra cluster...")
    try:
        max_wait = 180
        start_time = time.time()
        ready = False

        spinner = Halo(
            text="Waiting for Cassandra cluster to be ready (This can be multiple minutes)",
            spinner="dots",
        )
        spinner.start()

        while time.time() - start_time < max_wait:
            try:
                result = subprocess.run(
                    [
                        "sudo",
                        "docker",
                        "exec",
                        "cassandra-1",
                        "nodetool",
                        "status",
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=10,
                )

                if result.returncode == 0:
                    output = result.stdout
                    up_nodes = output.count("UN")
                    if up_nodes >= node_count:
                        ready = True
                        break
            except Exception:
                pass

            time.sleep(5)

        if ready:
            spinner.succeed(" Cassandra cluster ready")
        else:
            spinner.warn(
                "Cassandra cluster did not fully stabilize within timeout, try running: `sudo docker exec cassandra-1 nodetool status`, if no errors, the cluster is ready, so add more time in this function"
            )
    except Exception as e:
        print(f"Warning: Could not initialize Cassandra cluster: {e}")


def generate_cassandra_docker_compose(node_count, config):
    db_name = "cassandra"
    docker_compose_path = f"{db_name}/docker-compose-run.yml"

    with open(f"{db_name}/{config['DOCKER_COMPOSE_BASE_FILENAME']}", "r") as f:
        cassandra_yml = f.read()

    seeds = ",".join([f"cassandra-{j}" for j in range(1, node_count + 1)])
    for i in range(2, node_count + 1):
        port = 9042 + i - 1
        cassandra_yml += f"""
  cassandra-{i}:
    image: cassandra:latest
    container_name: cassandra-{i}
    ports:
      - "{port}:9042"
    environment:
      CASSANDRA_SEEDS: {seeds}
      CASSANDRA_CLUSTER_NAME: ycsb-cluster
      CASSANDRA_DC: dc1
      CASSANDRA_RACK: rack1
      CASSANDRA_LISTEN_ADDRESS: cassandra-{i}
      MAX_HEAP_SIZE: 256M
      HEAP_NEWSIZE: 50M
    healthcheck:
      test: ["CMD-SHELL", "[ $$(nodetool statusgossip) = running ]"]
      interval: 10s
      timeout: 10s
      retries: 10
    networks:
      - cassandra-net
    depends_on:
      cassandra-1:
        condition: service_healthy
"""

    cassandra_yml += """
networks:
  cassandra-net:
    driver: bridge
"""

    with open(docker_compose_path, "w") as f:
        f.write(cassandra_yml)


def create_cassandra_keyspace():
    print("Creating Cassandra keyspace and table...")
    try:
        cql_commands = """
CREATE KEYSPACE IF NOT EXISTS ycsb WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE ycsb;
CREATE TABLE IF NOT EXISTS usertable (y_id varchar PRIMARY KEY, field0 varchar, field1 varchar, field2 varchar, field3 varchar, field4 varchar, field5 varchar, field6 varchar, field7 varchar, field8 varchar, field9 varchar);
"""
        subprocess.run(
            [
                "sudo",
                "docker",
                "exec",
                "cassandra-1",
                "cqlsh",
                "-e",
                cql_commands,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        print("✓ Keyspace and table created")
    except Exception as e:
        print(f"Warning: Could not create keyspace/table: {e}")


def handle_cassandra_workload(
    workload_path, params, config, ycsb_wrapper, parse_ycsb_output, save_results_json
):
    results = {
        "workload": os.path.splitext(os.path.basename(params["workload_path"]))[0],
        "database": params["db"],
        "node_count": params["node_count"],
        "phases": [],
    }

    time.sleep(5)
    create_cassandra_keyspace()

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
