# scripts/run_client.py
import argparse
import asyncio
import logging
import sys
import os
import time
import grpc

# Assuming 'src' is in PYTHONPATH or running from root
from distributed_fs.generated import raft_pb2, raft_pb2_grpc

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - CLIENT - %(levelname)s - %(message)s')
logger = logging.getLogger("client")

# --- Command Formatting ---
# Let's use a simple format: OPERATION\nKEY\nVALUE (value is empty for GET/DEL)
# Using newline to avoid issues with spaces in keys/values.
# Alternative: JSON {"op": "PUT", "key": "my key", "value": "base64_encoded_value"} - more robust but more overhead.
COMMAND_SEPARATOR = b'\n'

def format_command(operation: str, key: str, value: bytes = b'') -> bytes:
    op = operation.upper().encode('utf-8')
    k = key.encode('utf-8')
    # Ensure no newlines in key
    if COMMAND_SEPARATOR in k:
        raise ValueError("Key cannot contain newline characters")
    if operation.upper() == "PUT":
        return op + COMMAND_SEPARATOR + k + COMMAND_SEPARATOR + value
    else: # GET or DELETE
        return op + COMMAND_SEPARATOR + k

def parse_reply(reply: raft_pb2.ClientCommandReply):
    """Parses and prints the reply, including value for GET."""
    if reply.success:
        logger.info(f"Success: {reply.message}")
        # Check if there's a value (for GET replies)
        if reply.value: # <<<--- CHECK IF VALUE EXISTS
             # Try decoding as UTF-8 for printing, fallback to hex for binary
             try:
                  print(f"Value: {reply.value.decode('utf-8')}")
             except UnicodeDecodeError:
                  print(f"Value (binary hex): {reply.value.hex()}")
        elif reply.message == "Key found": # Handle case where value might be empty string
             print("Value: (empty)")
    else:
        logger.error(f"Failed: {reply.message}")
        if reply.leader_hint:
            logger.info(f"Leader hint: {reply.leader_hint}")


async def run_command(initial_target: str, command_bytes: bytes, command_id: str):
    """Sends command, handles redirection, returns final reply."""
    target_address = initial_target
    max_redirects = 5 # Prevent infinite loops

    for attempt in range(max_redirects):
        logger.info(f"Attempt {attempt + 1}: Connecting to {target_address}...")
        try:
            # Create insecure channel
            async with grpc.aio.insecure_channel(target_address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                request = raft_pb2.ClientCommandRequest(
                    command=command_bytes,
                    command_id=command_id # Useful for deduplication (not implemented server-side yet)
                )

                # Add a reasonable timeout for the client call
                client_rpc_timeout = 15.0 # Seconds
                reply = await stub.ExecuteCommand(request, timeout=client_rpc_timeout)

                if reply.success or not reply.leader_hint:
                    # Success, or failure without a redirect hint -> return reply
                    logger.info(f"Received reply from {target_address}")
                    return reply
                else:
                    # Failure with a redirect hint
                    target_address = reply.leader_hint
                    logger.info(f"Redirected to leader: {target_address}. Retrying...")
                    # Add a small delay before retrying?
                    await asyncio.sleep(0.1)

        except grpc.aio.AioRpcError as e:
            logger.error(f"RPC error connecting to {target_address}: {e.code()} - {e.details()}")
            # Could retry on certain errors (e.g., UNAVAILABLE) or give up
            return None # Indicate failure
        except Exception as e:
            logger.error(f"Unexpected error communicating with {target_address}: {e}", exc_info=True)
            return None # Indicate failure

    logger.error(f"Failed to execute command after {max_redirects} attempts.")
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for the Distributed FS.")
    parser.add_argument("server_address", help="Address (host:port) of any server in the cluster.")
    parser.add_argument("command", choices=["put", "get", "delete"], help="Command to execute.")
    parser.add_argument("key", help="The key for the operation.")
    parser.add_argument("value", nargs='?', default="", help="The value for 'put' command (read from stdin if '-')")

    args = parser.parse_args()

    # --- Prepare command ---
    operation = args.command.upper()
    key = args.key
    value_bytes = b''

    if operation == "PUT":
        if args.value == "-":
            logger.info("Reading value from stdin...")
            value_bytes = sys.stdin.buffer.read() # Read raw bytes
        elif args.value:
            value_bytes = args.value.encode('utf-8')
        else:
            parser.error("Value is required for 'put' command (or use '-' for stdin).")

    try:
        command_bytes = format_command(operation, key, value_bytes)
    except ValueError as e:
        parser.error(str(e))

    # Generate a simple command ID (not guaranteed unique, but okay for demo)
    command_id = f"client_{os.getpid()}_{time.time_ns()}"
    logger.debug(f"Sending command: ID={command_id}, Op={operation}, Key='{key}', ValueSize={len(value_bytes)}")

    # --- Run Command ---
    async def main():
        reply = await run_command(args.server_address, command_bytes, command_id)
        if reply:
            parse_reply(reply)
            # Exit code based on success?
            if not reply.success:
                 sys.exit(1) # Indicate failure
        else:
            logger.error("Command execution failed (no reply or error).")
            sys.exit(1) # Indicate failure

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Client interrupted.")