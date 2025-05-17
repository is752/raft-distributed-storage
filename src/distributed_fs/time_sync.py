import time
import threading
import grpc
import logging
from concurrent import futures
from datetime import datetime
from distributed_fs.generated import time_sync_pb2, time_sync_pb2_grpc

# Proto imports would go here if you define a proto for time sync
# For demonstration, we'll use a simple Python gRPC service definition

class TimeSyncServicer(time_sync_pb2_grpc.TimeSyncServiceServicer):
    """A gRPC servicer for time synchronization."""
    def GetTime(self, request, context):
        # Return the current server time as a UNIX timestamp
        return time_sync_pb2.TimeReply(timestamp=time.time())

class TimeSynchronizer:
    def __init__(self, peers, interval=60, skew_threshold=2.0):
        """
        peers: list of (host, port) tuples
        interval: how often to sync (seconds)
        skew_threshold: warn if skew exceeds this (seconds)
        """
        self.peers = peers
        self.interval = interval
        self.skew_threshold = skew_threshold
        self.running = False
        self.thread = None
        self.logger = logging.getLogger("TimeSynchronizer")

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _run(self):
        while self.running:
            self.sync_with_peers()
            time.sleep(self.interval)

    def sync_with_peers(self):
        local_time = time.time()
        skews = []
        for host, port in self.peers:
            try:
                # Replace with actual gRPC call
                peer_time = self.get_peer_time(host, port)
                skew = peer_time - local_time
                skews.append(skew)
                self.logger.info(f"Peer {host}:{port} skew: {skew:.3f} seconds")
                if abs(skew) > self.skew_threshold:
                    self.logger.warning(f"Clock skew with {host}:{port} exceeds threshold: {skew:.3f} seconds")
            except Exception as e:
                self.logger.error(f"Failed to sync with {host}:{port}: {e}")
        if skews:
            avg_skew = sum(skews) / len(skews)
            self.logger.info(f"Average clock skew: {avg_skew:.3f} seconds")
        else:
            self.logger.warning("No peers responded for time sync.")

    def get_peer_time(self, host, port):
        # Make a real gRPC call to the peer's GetTime endpoint
        address = f"{host}:{port}"
        try:
            with grpc.insecure_channel(address) as channel:
                stub = time_sync_pb2_grpc.TimeSyncServiceStub(channel)
                request = time_sync_pb2.TimeRequest()
                response = stub.GetTime(request, timeout=2.0)  # 2 second timeout
                return response.timestamp
        except Exception as e:
            self.logger.error(f"gRPC error getting time from {address}: {e}")
            raise

def add_time_sync_service(server):
    """Helper to add the TimeSyncService to a gRPC server."""
    time_sync_pb2_grpc.add_TimeSyncServiceServicer_to_server(TimeSyncServicer(), server)

# Example usage (to be integrated into your server node):
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    peers = [("localhost", 50052), ("localhost", 50053)]  # Example peer list
    ts = TimeSynchronizer(peers, interval=10, skew_threshold=1.0)
    ts.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ts.stop() 