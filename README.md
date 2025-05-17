# Raft Distributed Storage System

A distributed storage system implementing the Raft consensus algorithm for fault tolerance and high availability.

## Overview

This project implements a distributed file system that is designed to be fault-tolerant. It leverages the Raft consensus algorithm to ensure data consistency and availability even when some nodes in the system fail.

## Features

*(To be filled in - e.g., specific capabilities of the file system)*

*   Fault tolerance via Raft consensus
*   Distributed storage
*   ...

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

## Setup

1. Create and activate a virtual environment:
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Cluster

The cluster is managed using the `manage_cluster.py` script. Here are the available commands:

### Starting the Cluster

To start all nodes in the cluster:
```bash
python scripts/manage_cluster.py start-all
```

To start a specific node:
```bash
python scripts/manage_cluster.py start <node_id>
```

### Stopping the Cluster

To stop all nodes in the cluster:
```bash
python scripts/manage_cluster.py stop-all
```

To stop a specific node:
```bash
python scripts/manage_cluster.py stop <node_id>
```

You can also specify the signal to use when stopping nodes:
```bash
python scripts/manage_cluster.py stop-all --sig TERM  # SIGTERM (default)
python scripts/manage_cluster.py stop-all --sig INT   # SIGINT
python scripts/manage_cluster.py stop-all --sig KILL  # SIGKILL
```

### Checking Cluster Status

To check the status of all nodes:
```bash
python scripts/manage_cluster.py status
```

### Viewing Logs

To view logs for a specific node:
```bash
python scripts/manage_cluster.py logs <node_id>
```

To view a specific number of lines:
```bash
python scripts/manage_cluster.py logs <node_id> -n 50
```

## Interacting with the Cluster

Use the `run_client.py` script to interact with the cluster. The client supports three operations: PUT, GET, and DELETE.

### Basic Commands

1. PUT - Store a value:
```bash
python scripts/run_client.py <server_address> put <key> <value>
```

2. GET - Retrieve a value:
```bash
python scripts/run_client.py <server_address> get <key>
```

3. DELETE - Remove a value:
```bash
python scripts/run_client.py <server_address> delete <key>
```

### Examples

1. Store a value:
```bash
python scripts/run_client.py localhost:8001 put mykey "Hello, World!"
```

2. Retrieve a value:
```bash
python scripts/run_client.py localhost:8001 get mykey
```

3. Delete a value:
```bash
python scripts/run_client.py localhost:8001 delete mykey
```

4. Store binary data from a file:
```bash
cat myfile.bin | python scripts/run_client.py localhost:8001 put myfile -
```

## Configuration

The cluster configuration is stored in `cluster_config.json`. This file contains settings for:
- Number of nodes
- Base port (starts from 8001)
- Host address
- Data directory locations
- Log directory locations
- PID file locations

## Notes

- The client will automatically handle leader redirection if it connects to a follower node
- Each command has a timeout of 15 seconds
- The client will retry up to 5 times if redirected to the leader
- Logs for each node are stored in the configured log directory
- PID files are used to track running nodes and are stored in the configured PID directory
- Node ports start from 8001 and increment for each additional node (8001, 8002, 8003, etc.)

## Project Structure

```
cursor-pyds/
├── proto/            # Protocol buffer definitions
├── pyproject.toml    # Project metadata and build configuration
├── requirements.txt  # Project dependencies
├── scripts/          # Utility scripts
├── src/              # Source code
│   └── distributed_fs/ # Main package for the distributed file system
│       ├── generated/  # Generated code from .proto files
│       ├── raft/       # Raft consensus algorithm implementation
│       └── ...         # Other modules
├── tests/            # Unit and integration tests
└── venv/             # Python virtual environment (if created)
```

## Development

*(Information for developers wanting to contribute or understand the codebase)*

### Setting up the development environment

Beyond the standard installation, for development you might need additional tools, especially for gRPC code generation.
*   Ensure `grpcio-tools` is installed. You can typically install it via pip:
    ```bash
    pip install grpcio-tools
    ```
    Consider adding this to a `requirements-dev.txt` if you create one.

### Running tests
*(Assuming pytest is used, update if different)*
```bash
pytest tests/
```

### Generating gRPC code
The project uses gRPC and Protocol Buffers. Interface definitions are located in `.proto` files within the `proto/` directory. If you modify these files, you must regenerate the corresponding Python gRPC stubs and message classes. These generated files are placed in `src/distributed_fs/generated/`.

**Command for regenerating gRPC code:**

You'll need `grpcio-tools` installed (see above). The following command can be run from the root of the project:
```bash
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src/distributed_fs/generated \
    --pyi_out=./src/distributed_fs/generated \
    --grpc_python_out=./src/distributed_fs/generated \
    ./proto/*.proto
```
*   `-I./proto`: Specifies the directory where your `.proto` files are located.
*   `--python_out`: Specifies the directory for the generated Python message classes.
*   `--pyi_out`: Specifies the directory for the generated Python type stub files (`.pyi`).
*   `--grpc_python_out`: Specifies the directory for the generated Python gRPC client and server stubs.
*   `./proto/*.proto`: Specifies all `.proto` files in the `proto` directory to be processed.

Consider adding this command to a helper script in the `scripts/` directory for convenience.