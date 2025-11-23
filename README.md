# Comparison of NoSQL Databases (Redis, MongoDB and Apache Cassandra)

## Table of Contents

- [Comparison of NoSQL Databases (Redis, MongoDB and Apache Cassandra)](#comparison-of-nosql-databases-redis-mongodb-and-apache-cassandra)
  - [Table of Contents](#table-of-contents)
  - [Installing](#installing)
    - [Prerequisites](#prerequisites)
    - [Step 0: Install Docker](#step-0-install-docker)
    - [Step 1: Install YCSB](#step-1-install-ycsb)
    - [Step 2: Add YCSB to PATH](#step-2-add-ycsb-to-path)
    - [Step 3: Set up Python virtual environment](#step-3-set-up-python-virtual-environment)
    - [Step 4: Update run.py configuration](#step-4-update-runpy-configuration)
  - [Using](#using)
    - [Basic Usage](#basic-usage)
    - [Arguments](#arguments)
    - [Examples](#examples)
    - [Output](#output)
    - [Workload Files](#workload-files)
      - [Built-in](#built-in)
      - [Custom](#custom)

## Installing

I'm using an ARM's based machine on Ubuntu 20 LTS. - Mathieu

Works pretty well with [WSL](https://learn.microsoft.com/en-us/windows/wsl/install). - Steven

### Prerequisites

-   Docker and Docker Compose
-   Python 3
-   Java 8+ (required by YCSB)

### Step 0: Install Docker

Ubuntu/Debian:

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
rm get-docker.sh
```

Docker Compose is included with Docker. Verify it works:

```bash
docker compose --version
```

Add your user to docker group (no sudo needed):

```bash
sudo usermod -aG docker $USER
newgrp docker
```

**Log out and log back in**

Verify Docker works without sudo:

```bash
docker ps
```

**Log out and log back in if the above command fails**. I'm not your mom, do it how you want. (The sure-fire way would be to do: `sudo reboot`)

### Step 1: Install YCSB

```bash
cd ~
wget https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar -xzf ycsb-0.17.0.tar.gz
```

### Step 2: Add YCSB to PATH

```bash
echo 'export PATH="$HOME/ycsb-0.17.0/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### Step 3: Set up Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 4: Update run.py configuration

If necessary, edit `run.py` and update the `YCSB_BIN_PATH` to use the PATH version:

```python
CONFIG = {
    "YCSB_BIN_PATH": "ycsb.sh",
    ...
}
```

## Using

### Basic Usage

```bash
python3 run.py <database> <node_count> <workload> [iterations] [--keep-alive]
```

### Arguments

-   `<database>`: redis, mongodb, or cassandra
-   `<node_count>`: Number of nodes (positive integer)
-   `<workload>`: Workload name from workloads/ directory ([see below](#workload-files))
-   `[iterations]`: Optional - Number of run iterations (default: 1)
-   `[--keep-alive]`: Optional - Keep containers running after exit (clean up by default)

### Examples

Single Redis node with workload A:

```bash
python3 run.py redis 1 workloada
```

3-node MongoDB cluster, workload B, 5 iterations, keep containers running

```bash
python3 run.py mongodb 3 workloadb 5 --keep-alive
```

Cassandra with 2 nodes and a custom workload `im_a_custom_workload`:

```bash
python3 run.py cassandra 2 im_a_custom_workload
```

### Output

Results are saved as JSON files in `results/<database>/<node_count>/<workload>.json` containing:

### Workload Files

#### Built-in

YCSB includes a set of [built-in](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads) workloads.

They can be found using:

```
ls ~/ycsb-0.17.0/workloads (~ or depending on where you installed YCSB)
```

For simplicity, the relevant workloads for the assignment (A-E) have been copied to the `workloads/` directory.

| Workload Label | YCSB File | Description                                   |
| -------------- | --------- | --------------------------------------------- |
| Workload A     | workloada | Update-heavy workload (Read: 50%, Write: 50%) |
| Workload B     | workloadb | Read-heavy workload (Read: 95%, Write: 5%)    |
| Workload C     | workloadc | Read-only workload (Read: 100%)               |
| Workload D     | workloadd | Read latest data (Read: 95%, Insert: 5%)      |
| Workload E     | workloade | Short ranges workload (Scan: 95%, Insert: 5%) |

#### Custom

Custom workloads can also be added to the `workloads/` directory. Example workload properties:

```
recordcount=1000
operationcount=10000
readproportion=0.5
updateproportion=0.5
...
```
