# Distributed FS

A Fault-Tolerant Distributed File Storage System using Raft.

## Overview

This project implements a distributed file system that is designed to be fault-tolerant. It leverages the Raft consensus algorithm to ensure data consistency and availability even when some nodes in the system fail.

## Features

*(To be filled in - e.g., specific capabilities of the file system)*

*   Fault tolerance via Raft consensus
*   Distributed storage
*   ...

## Prerequisites

*   Python 3.8 or higher
*   *(List other prerequisites, if any, e.g., specific OS, tools)*

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd cursor-pyds
    ```

2.  **Set up a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    Alternatively, if all dependencies are listed in `pyproject.toml` under `[project.dependencies]`, you might be able to install with:
    ```bash
    pip install .
    ```

## Usage

The system consists of server nodes that form a Raft cluster and client scripts to interact with the distributed file system.

### Running Server Nodes

Each server node in the Raft cluster must be started using the `scripts/run_server.py` script.

**Arguments:**
*   `--id <node_id>`: (Required) Unique integer ID for this node.
*   `--port <port_number>`: (Required) Port for this node to listen on.
*   `--peers <peer_list>`: (Required) Comma-separated list of all peer nodes in the cluster, including itself. The format for each peer is `id@host:port`.
    *   Example: `1@localhost:50051,2@localhost:50052,3@localhost:50053`
*   `--data-dir <path>`: (Required) Directory to store Raft log, state, and snapshots for this node. Create a unique directory for each node.
*   `--debug`: (Optional) Enable debug logging.

**Example: Starting a 3-node cluster locally**

You'll need three separate terminals.

**Node 1:**
```bash
python scripts/run_server.py --id 1 --port 50051 --peers 1@localhost:50051,2@localhost:50052,3@localhost:50053 --data-dir ./node1_data
```

**Node 2:**
```bash
python scripts/run_server.py --id 2 --port 50052 --peers 1@localhost:50051,2@localhost:50052,3@localhost:50053 --data-dir ./node2_data
```

**Node 3:**
```bash
python scripts/run_server.py --id 3 --port 50053 --peers 1@localhost:50051,2@localhost:50052,3@localhost:50053 --data-dir ./node3_data
```
Make sure to create the `node1_data`, `node2_data`, and `node3_data` directories beforehand or modify the script to create them.

### Running the Client

Interact with the distributed file system using the `scripts/run_client.py` script.

**Arguments:**
*   `server_address`: (Required) Address (`host:port`) of *any* server node in the cluster.
*   `command`: (Required) The command to execute. Choices: `put`, `get`, `delete`.
*   `key`: (Required) The key for the operation.
*   `value`: (Optional) The value for the `put` command.
    *   If providing directly: `python scripts/run_client.py localhost:50051 put mykey "my value"`
    *   If reading from stdin: `echo "my value" | python scripts/run_client.py localhost:50051 put mykey -`

**Client Examples:**

*   **Put a key-value pair:**
    ```bash
    python scripts/run_client.py localhost:50051 put mykey "hello world"
    ```
    Or, using stdin for the value:
    ```bash
    echo "this is a larger value from stdin" | python scripts/run_client.py localhost:50051 put anotherkey -
    ```

*   **Get a value by key:**
    ```bash
    python scripts/run_client.py localhost:50051 get mykey
    ```

*   **Delete a key:**
    ```bash
    python scripts/run_client.py localhost:50051 delete anotherkey
    ```
The client will automatically handle redirection if it initially contacts a follower node.

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

## Contributing

Contributions are welcome! Please follow these steps:

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature-name`).
3.  Make your changes.
4.  Commit your changes (`git commit -am 'Add some feature'`).
5.  Push to the branch (`git push origin feature/your-feature-name`).
6.  Create a new Pull Request.

Please make sure to update tests as appropriate.

## License

*(To be filled in - e.g., MIT, Apache 2.0). If no license is chosen yet, state that.*

This project is currently unlicensed.