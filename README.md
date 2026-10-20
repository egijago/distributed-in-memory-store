# Distributed in Memory Store

**Distributed in Memory Store** is a distributed, in-memory key-value store built on the **Raft consensus algorithm**. It provides a Redis-like interface, ensuring strong consistency and high availability across a cluster of nodes.

Built with **Go**, it leverages **gRPC** for node-to-node communication and **gRPC-Web** for browser-based client interaction.

## Features

* **Raft Consensus:** Implements Leader Election and Log Replication using Raft Algorithm.
* **2 Phase Commit:** Implements Membership Change using 2 Phase Commit.
* **In-Memory Storage:** Fast key-value access.
* **Dual Interfaces:**
    * **gRPC:** For high-performance internal communication and backend clients.
    * **gRPC-Web:** Native support for web clients without a proxy.
* **Configurable Timings:** Fine-grained control over election timeouts, heartbeats, and two-phase commit timeouts.
* **Observability:** Built-in verbose logging for state transitions.

## Prerequisites

* **Go** (1.19+)
* **Protobuf Compiler (`protoc`)**
* **Make**

## Installation

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/egijago/redraft.git](https://github.com/egijago/redraft.git)
    cd redraft
    ```

2.  **Generate Protocol Buffers:**
    This project uses gRPC. You must generate the Go code from the `.proto` definitions before building.
    ```bash
    make proto
    ```
    *(Optional) Generate client-side JavaScript/TypeScript stubs:*
    ```bash
    make proto-client
    ```

3.  **Build the binary:**
    ```bash
    make build
    ```

## Usage

You can run nodes individually using `go run` or utilize the helper commands in the `Makefile`.

### Starting a Cluster (Quick Start)

The `Makefile` contains pre-configured recipes to start a local 3-node cluster. Open three separate terminal tabs:

**Terminal 1 (The Initial Leader):**
```bash
# Starts on gRPC port 8000
make node0
```

**Terminal 2 (Follower 1):**
```bash
# Connects to leader at port 8000
make node1
```


**Terminal 3 (Follower 2):**
```bash
# Connects to leader at port 8000
make node2
```

Or simply, run the `docker compose up` if you have docker in your system.

### Manual Configuration
To start a node manually, use the `make node` command or run the binary directly.
Here are the configuration flags to be use: 

| Flag             | Default     | Description                                           |
|------------------|------------|-------------------------------------------------------|
| `--host`         | `localhost` | The hostname for this node.                          |
| `--grpc_port`    | (Required)  | Port for gRPC traffic (Node-to-Node).                |
| `--grpc_web_port`| (Required)  | Port for gRPC-Web traffic (Client-to-Node).          |
| `--master_host`  | `localhost` | Hostname of the cluster leader (for followers).      |
| `--master_port`  | `""`        | Port of the cluster leader. Leave empty to start as Leader. |
| `--verbose`      | `false`     | Enable periodic state logging.                       |
| `--qs`           | `0.5`       | Quorum scale (0.0 - 1.0).                            |


**Raft Tuning Flags**
| Flag     | Description                          |
|---------|--------------------------------------|
| `--et`   | Election timeout (ms)                |
| `--hi`   | Heartbeat interval (ms)              |
| `--hrto` | Heartbeat request timeout (ms)       |
| `--ht`   | Heartbeat timeout (ms)               |
| `--hts`  | Heartbeat timeout slack (ms)         |
| `--tpct` | Two-phase commit timeout (ms)        |
