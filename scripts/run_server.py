# scripts/run_server.py
import argparse
import asyncio
import logging
import signal
from concurrent import futures
from typing import List, Tuple

import grpc

# Import generated gRPC files (adjust path based on your structure)
# Assuming 'src' is in the Python path or you run from the root dir
from distributed_fs.generated import raft_pb2, raft_pb2_grpc

# Import the RaftNode class (we'll create this next)
from distributed_fs.raft.node import RaftNode

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- gRPC Service Implementation ---
class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    """
    The gRPC service implementation. It delegates the actual Raft logic
    to the RaftNode instance.
    """
    def __init__(self, raft_node: RaftNode):
        self.raft_node = raft_node
        logger.info("RaftServicer initialized.")

    async def RequestVote(self, request: raft_pb2.RequestVoteArgs, context) -> raft_pb2.RequestVoteReply:
        logger.debug(f"Received RequestVote call from {request.candidate_id} for term {request.term}")
        # Delegate the call to the RaftNode's handler method
        return await self.raft_node.handle_request_vote(request)

    async def AppendEntries(self, request: raft_pb2.AppendEntriesArgs, context) -> raft_pb2.AppendEntriesReply:
        logger.debug(f"Received AppendEntries call from {request.leader_id} for term {request.term}")
        # Delegate the call to the RaftNode's handler method
        return await self.raft_node.handle_append_entries(request)

    async def ExecuteCommand(self, request: raft_pb2.ClientCommandRequest, context) -> raft_pb2.ClientCommandReply:
        logger.debug(f"Received ExecuteCommand call")
        # Delegate the call to the RaftNode's handler method
        return await self.raft_node.handle_client_command(request)

# --- Server Startup and Shutdown ---
async def serve(raft_node: RaftNode, port: int):
    """Starts the asynchronous gRPC server."""
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(
        RaftServicer(raft_node), server
    )
    listen_addr = f'[::]:{port}' # Listen on all interfaces (IPv6 compatible)
    server.add_insecure_port(listen_addr)

    logger.info(f"Starting server on {listen_addr}")
    await server.start()

    # Start the Raft node's background tasks (like timers)
    # We use create_task to run it concurrently with the server
    raft_task = asyncio.create_task(raft_node.run())

    # Graceful shutdown logic
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

    await stop # Wait for shutdown signal

    logger.info("Shutdown signal received. Stopping server and Raft node...")
    # Stop the Raft node first
    await raft_node.stop()
    raft_task.cancel() # Ensure the task finishes if stop() didn't

    # Then stop the gRPC server
    await server.stop(grace=1) # Allow 1 second for ongoing calls
    logger.info("Server stopped.")

# --- Argument Parsing and Main Execution ---
def parse_peers(peer_list_str: str) -> List[Tuple[int, str]]:
    """Parses comma-separated peer list like '1@host1:port1,2@host2:port2'"""
    peers = []
    if not peer_list_str:
        return peers
    for peer_str in peer_list_str.split(','):
        try:
            id_str, addr = peer_str.strip().split('@')
            peer_id = int(id_str)
            peers.append((peer_id, addr))
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid peer format: '{peer_str}'. Expected 'id@host:port'.")
    return peers

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run a Raft node.")
    parser.add_argument("--id", type=int, required=True, help="Unique ID of this node")
    parser.add_argument("--port", type=int, required=True, help="Port for this node to listen on")
    parser.add_argument("--peers", type=str, required=True, help="Comma-separated list of all peer nodes in the format 'id@host:port'")
    parser.add_argument("--data-dir", type=str, required=True, help="Directory to store Raft log and state")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse the peer list string into a usable format
    try:
        peers_list = parse_peers(args.peers)
    except argparse.ArgumentTypeError as e:
        parser.error(str(e)) # Exit if parsing fails

    # Find our own address from the peer list (or ensure it's consistent)
    # You might want more robust validation here later
    my_addr = None
    peer_addresses = {}
    for pid, addr in peers_list:
        peer_addresses[pid] = addr
        if pid == args.id:
            # Extract host/port logic might be needed if using 0.0.0.0 vs specific IP
            # For now, assume the port matches
            split_addr = addr.split(':')
            if len(split_addr) != 2 or not split_addr[1].isdigit() or int(split_addr[1]) != args.port:
                 parser.error(f"Address '{addr}' for own ID {args.id} in peers list doesn't match provided port {args.port}")
            my_addr = addr # We found our own configured address

    if my_addr is None:
        parser.error(f"Own ID {args.id} not found in the peers list: {args.peers}")

    # Create the RaftNode instance
    raft_node = RaftNode(
        node_id=args.id,
        peers_addresses=peer_addresses, # Pass the dict: {peer_id: "host:port", ...}
        data_dir=args.data_dir
    )

    # Run the server using asyncio
    try:
        asyncio.run(serve(raft_node, args.port))
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")