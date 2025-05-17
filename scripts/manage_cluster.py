# scripts/manage_cluster.py
import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

CONFIG_FILE = "cluster_config.json" # Default config file name

def load_config(config_path):
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Configuration file '{config_path}' contains invalid JSON.")
        sys.exit(1)

def get_node_params(config, node_id):
    port = config["base_port"] + node_id - 1
    data_dir = Path(config["base_data_dir_parent"]) / f"node{node_id}"
    log_file = Path(config["base_log_dir_parent"]) / f"node{node_id}.log"
    pid_file = Path(config["pid_dir"]) / f"node{node_id}.pid"

    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(log_file.parent, exist_ok=True)
    os.makedirs(pid_file.parent, exist_ok=True)

    peers = []
    for i in range(1, config["num_nodes"] + 1):
        peer_port = config["base_port"] + i - 1
        peers.append(f"{i}@{config['host']}:{peer_port}")
    peers_str = ",".join(peers)

    return port, data_dir, log_file, peers_str, pid_file

def start_node(config, node_id):
    port, data_dir, log_file, peers_str, pid_file = get_node_params(config, node_id)

    if pid_file.exists():
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
            os.kill(pid, 0) # Check if process exists
            print(f"Node {node_id} (PID {pid}) seems to be already running.")
            return
        except (OSError, ValueError): # Process not running or PID file stale
            pid_file.unlink()

    cmd_args = [
        config["python_executable"],
        config["run_server_script"],
        "--id", str(node_id),
        "--port", str(port),
        "--peers", peers_str,
        "--data-dir", str(data_dir)
    ]
    if config.get("debug_logging", False):
        cmd_args.append("--debug")

    print(f"Starting Node {node_id}: {' '.join(cmd_args)}")
    print(f"  Log file: {log_file}")
    print(f"  Data dir: {data_dir}")
    print(f"  PID file: {pid_file}")

    with open(log_file, 'ab') as lf: # Append binary for direct Popen output
        # Use shlex.split if any arguments might contain spaces, though not strictly needed here
        process = subprocess.Popen(cmd_args, stdout=lf, stderr=subprocess.STDOUT, start_new_session=True)

    try:
        with open(pid_file, 'w') as pf:
            pf.write(str(process.pid))
        print(f"Node {node_id} started with PID {process.pid}.")
    except Exception as e:
        print(f"Error writing PID file for node {node_id}: {e}")
        process.terminate() # Try to kill it if PID file fails
        process.wait()

def stop_node(config, node_id, sig=signal.SIGTERM):
    _port, _data_dir, _log_file, _peers_str, pid_file = get_node_params(config, node_id)

    if not pid_file.exists():
        print(f"Node {node_id}: PID file not found. Is it running or was it started by this manager?")
        return

    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
    except ValueError:
        print(f"Node {node_id}: Invalid PID in PID file {pid_file}. Removing stale file.")
        pid_file.unlink(missing_ok=True)
        return
    except FileNotFoundError: # Should be caught by initial check, but for safety
        print(f"Node {node_id}: PID file disappeared.")
        return


    print(f"Stopping Node {node_id} (PID {pid}) with signal {sig.name}...")
    try:
        os.kill(pid, sig)
        # Wait a bit for the process to terminate
        time.sleep(1) # Give it a moment
        try:
            os.kill(pid, 0) # Check if still alive
            print(f"Node {node_id} (PID {pid}) did not terminate gracefully with {sig.name}. Consider 'kill -9 {pid}' manually or using SIGKILL option.")
        except OSError: # Process is dead
            print(f"Node {node_id} (PID {pid}) stopped.")
            pid_file.unlink(missing_ok=True)
    except OSError as e:
        if e.errno == errno.ESRCH: # No such process
            print(f"Node {node_id} (PID {pid}) not found or already stopped.")
            pid_file.unlink(missing_ok=True)
        else:
            print(f"Error stopping node {node_id} (PID {pid}): {e}")


def start_all(config):
    print(f"Starting all {config['num_nodes']} nodes...")
    for i in range(1, config["num_nodes"] + 1):
        start_node(config, i)
        time.sleep(0.2) # Small delay between starts

def stop_all(config, sig=signal.SIGTERM):
    print(f"Stopping all {config['num_nodes']} nodes with signal {sig.name}...")
    # Stop in reverse order, often helps leader step down more cleanly (though not strictly necessary)
    for i in range(config["num_nodes"], 0, -1):
        stop_node(config, i, sig)
        time.sleep(0.1)

def cluster_status(config):
    print("Cluster Status:")
    running_nodes = 0
    for i in range(1, config["num_nodes"] + 1):
        _port, _data_dir, _log_file, _peers_str, pid_file = get_node_params(config, i)
        status = "Stopped"
        pid_val = "N/A"
        if pid_file.exists():
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                os.kill(pid, 0) # Check if process exists
                status = f"Running (PID: {pid})"
                pid_val = pid
                running_nodes +=1
            except (OSError, ValueError):
                status = "Stopped (Stale PID file)"
                # pid_file.unlink(missing_ok=True) # Optionally clean up stale PID files here
        print(f"  Node {i}: {status}")
    print(f"Total running (according to PIDs): {running_nodes}/{config['num_nodes']}")


def tail_log(config, node_id, lines=20):
    _port, _data_dir, log_file, _peers_str, _pid_file = get_node_params(config, node_id)
    if not log_file.exists():
        print(f"Log file for Node {node_id} ({log_file}) does not exist.")
        return
    print(f"Tailing log for Node {node_id} ({log_file}). Press Ctrl+C to stop.")
    try:
        # subprocess.run(['tail', '-n', str(lines), '-f', str(log_file)], check=True)
        # A simpler Python way to tail for demonstration, less robust than `tail -f`
        with open(log_file, 'r') as f:
            # Print last N lines
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            buffer_size = 1024 * lines # Estimate
            read_pos = max(0, file_size - buffer_size)
            f.seek(read_pos)
            content = f.read()
            last_lines = content.splitlines()[-lines:]
            for line in last_lines:
                print(line)

            # Follow
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                print(line.strip())

    except KeyboardInterrupt:
        print(f"\nStopped tailing log for Node {node_id}.")
    except FileNotFoundError:
        print(f"Log file {log_file} not found.")
    except Exception as e:
        print(f"Error tailing log: {e}")


def main():
    parser = argparse.ArgumentParser(description="Manage a local Raft cluster for testing.")
    parser.add_argument("--config", default=CONFIG_FILE, help=f"Path to cluster configuration file (default: {CONFIG_FILE})")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Command to execute")

    start_all_parser = subparsers.add_parser("start-all", help="Start all nodes in the cluster.")
    stop_all_parser = subparsers.add_parser("stop-all", help="Stop all nodes in the cluster.")
    stop_all_parser.add_argument("--sig", default="TERM", choices=["TERM", "INT", "KILL"], help="Signal to use (TERM, INT, KILL)")


    start_node_parser = subparsers.add_parser("start", help="Start a specific node.")
    start_node_parser.add_argument("node_id", type=int, help="ID of the node to start.")

    stop_node_parser = subparsers.add_parser("stop", help="Stop a specific node.")
    stop_node_parser.add_argument("node_id", type=int, help="ID of the node to stop.")
    stop_node_parser.add_argument("--sig", default="TERM", choices=["TERM", "INT", "KILL"], help="Signal to use (TERM, INT, KILL)")


    status_parser = subparsers.add_parser("status", help="Show the status of nodes in the cluster.")

    logs_parser = subparsers.add_parser("logs", help="Tail the log file of a specific node.")
    logs_parser.add_argument("node_id", type=int, help="ID of the node whose logs to tail.")
    logs_parser.add_argument("-n", "--lines", type=int, default=20, help="Number of initial lines to show.")


    args = parser.parse_args()
    config = load_config(args.config)

    signal_map = {
        "TERM": signal.SIGTERM,
        "INT": signal.SIGINT,
        "KILL": signal.SIGKILL,
    }

    if args.command == "start-all":
        start_all(config)
    elif args.command == "stop-all":
        sig_to_send = signal_map.get(args.sig.upper(), signal.SIGTERM)
        stop_all(config, sig=sig_to_send)
    elif args.command == "start":
        start_node(config, args.node_id)
    elif args.command == "stop":
        sig_to_send = signal_map.get(args.sig.upper(), signal.SIGTERM)
        stop_node(config, args.node_id, sig=sig_to_send)
    elif args.command == "status":
        cluster_status(config)
    elif args.command == "logs":
        tail_log(config, args.node_id, args.lines)


if __name__ == "__main__":
    import errno # For os.kill error checking in stop_node
    main()
