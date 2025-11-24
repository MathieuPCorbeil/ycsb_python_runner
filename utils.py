import math
import os

from scipy import stats

from config import CONFIG


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


def validate_workload_path(workload_file):
    """Validate and return the full workload path."""
    workload_path = f"{CONFIG['WORKLOADS_PATH']}/{workload_file}"
    if not os.path.exists(workload_path):
        raise ValueError(f"Workload file does not exist: {workload_path}")
    return workload_path


def aggregate_metric(values):
    """Compute mean, standard deviation and 95% confidence interval."""
    n = len(values)
    mean = sum(values) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in values) / (n - 1)) if n > 1 else 0
    ci = 0
    if n > 1:
        t = stats.t.ppf(0.975, n - 1)  # 95% CI, two-tailed
        ci = t * sd / math.sqrt(n)
    return {
        "mean": mean,
        "sd": sd,
        "95ci": (mean - ci, mean + ci),
    }
