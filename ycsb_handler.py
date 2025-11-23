import re
import subprocess

from config import CONFIG, params


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
