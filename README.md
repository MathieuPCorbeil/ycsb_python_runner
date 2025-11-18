I'm using an ARM's based machine on Ubuntu 20 LTS.

## Installing

### Prerequisites

- Docker and Docker Compose
- Python 3
- Java 8+ (required by YCSB)

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

Edit `run.py` and update the `YCSB_BIN_PATH` to use the PATH version:

```python
CONFIG = {
    "YCSB_BIN_PATH": "ycsb.sh",
    ...
}
```

## Using

### Basic Usage

```bash
python3 run.py <database> <node_count> <workload> [iterations] [-c]
```

### Arguments

- `<database>`: redis, mongodb, or cassandra
- `<node_count>`: Number of nodes (positive integer)
- `<workload>`: Workload name from workloads/ directory (e.g., update_heavy, read_heavy)
- `[iterations]`: Optional - Number of run iterations (default: 1)
- `[-c]`: Optional - Clean up orphan containers before starting (use when changing `node_count`)

### Examples

Single node Redis with update_heavy workload:

```bash
python3 run.py redis 1 update_heavy
```

3-node MongoDB cluster, 5 iterations, cleanup orphans:

```bash
python3 run.py mongodb 3 update_heavy 5 -c
```

Cassandra with 2 nodes:

```bash
python3 run.py cassandra 2 read_heavy -c
```

### Output

Results are saved as JSON files in `results/<database>/<node_count>/<workload>.json` containing:

### Workload Files

Place custom workload files in the `workloads/` directory. Example workload properties:

```
recordcount=1000
operationcount=10000
readproportion=0.5
updateproportion=0.5
...
```
