import os

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
