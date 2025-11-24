import subprocess

from cassandra.cassandra_operations import (
    generate_cassandra_docker_compose,
    wait_for_cassandra_cluster_init,
)
from config import CONFIG, params
from mongodb.mongodb_operations import (
    generate_mongodb_docker_compose,
    initialize_mongodb_replica_set,
)
from redis.redis_operations import (
    generate_redis_docker_compose,
)


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
        wait_for_cassandra_cluster_init(params["node_count"])


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
