# src/distributed_fs/raft/node.py
import json
import struct
import grpc
import asyncio
import logging
import random
import os
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

# Import generated gRPC types
from distributed_fs.generated import raft_pb2, raft_pb2_grpc

# --- Constants ---
ELECTION_TIMEOUT_MIN_MS = 800
ELECTION_TIMEOUT_MAX_MS = 950
HEARTBEAT_INTERVAL_MS = 50

# --- Enums ---
class NodeState(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()

# --- Raft Node Class ---
logger = logging.getLogger(__name__)

class RaftNode:
    def __init__(self, node_id: int, peers_addresses: Dict[int, str], data_dir: str):
        self.node_id = node_id
        self.peers_addresses = peers_addresses # Dict {peer_id: "host:port"}
        self.data_dir = data_dir
        self.peer_stubs = {} # To store gRPC client stubs {peer_id: RaftServiceStub}

        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)

        # --- File Paths ---
        self.state_file_path = os.path.join(self.data_dir, "raft_state.json")
        self.log_file_path = os.path.join(self.data_dir, "raft.log")

        # === Persistent state on all servers ===
        # (Updated on stable storage before responding to RPCs)
        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.log: List[raft_pb2.LogEntry] = [] # In-memory for now
        # Load persistent state from disk
        self._load_state()
        self._load_log()

        # === Volatile state on all servers ===
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.state: NodeState = NodeState.FOLLOWER
        self.leader_id: Optional[int] = None # Track current known leader
        self.kv_store: Dict[str, bytes] = {} # Simple in-memory K/V store

        # === Volatile state on leaders ===
        # (Reinitialized after election)
        self.next_index: Dict[int, int] = {} # For each server, index of the next log entry to send
        self.match_index: Dict[int, int] = {} # For each server, index of highest log entry known to be replicated

        # === Internal state ===
        self._election_timer: Optional[asyncio.TimerHandle] = None
        self._heartbeat_timer: Optional[asyncio.Task] = None # Task for periodic heartbeats
        # self._loop = asyncio.get_running_loop() # remove this line
        self._votes_received: set = set() # Used during candidate state
        # Tracks futures for client requests waiting for commit {log_index: asyncio.Future}
        self._commit_futures: Dict[int, asyncio.Future] = {}
        # Lock for safely accessing/modifying _commit_futures
        self._commit_futures_lock = asyncio.Lock()

        logger.info(f"Node {self.node_id}: Initialized in Follower state. Term: {self.current_term}")
        logger.info(f"Node {self.node_id}: Peers: {self.peers_addresses}")


    # --- Persistence Methods ---

    def _persist_state(self):
        """Saves current_term and voted_for to the state file."""
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
        }
        try:
            # Write atomically: write to temp file, then rename
            temp_path = self.state_file_path + ".tmp"
            with open(temp_path, 'w') as f:
                json.dump(state, f)
                f.flush() # Ensure Python's buffer is flushed to OS
                os.fsync(f.fileno()) # Ensure OS buffer is flushed to disk
            os.rename(temp_path, self.state_file_path)
            logger.debug(f"Node {self.node_id}: Persisted state: term={self.current_term}, voted_for={self.voted_for}")
        except IOError as e:
            logger.error(f"Node {self.node_id}: Failed to persist state to {self.state_file_path}: {e}", exc_info=True)
            # Critical error, might need to handle more robustly (e.g., crash?)

    def _load_state(self):
        """Loads current_term and voted_for from the state file."""
        try:
            if os.path.exists(self.state_file_path):
                with open(self.state_file_path, 'r') as f:
                    state = json.load(f)
                    self.current_term = state.get("current_term", 0)
                    self.voted_for = state.get("voted_for", None)
                    logger.info(f"Node {self.node_id}: Loaded state: term={self.current_term}, voted_for={self.voted_for}")
            else:
                logger.info(f"Node {self.node_id}: State file not found. Initializing state.")
                # Initial state is term 0, voted_for None (already defaults)
                # Persist initial state? Good practice.
                self._persist_state()
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Node {self.node_id}: Failed to load state from {self.state_file_path}: {e}. Starting with default state.", exc_info=True)
            # Reset to default state in case of corruption
            self.current_term = 0
            self.voted_for = None
            # Attempt to persist default state to potentially fix corruption for next boot
            self._persist_state()


    def _persist_log_entry(self, entry: raft_pb2.LogEntry):
        """Appends a single serialized log entry (with length prefix) to the log file."""
        try:
            entry_bytes = entry.SerializeToString()
            # Prefix with 4-byte length (unsigned int, network byte order)
            length_prefix = struct.pack("!I", len(entry_bytes))
            with open(self.log_file_path, 'ab') as f: # Append binary mode
                f.write(length_prefix)
                f.write(entry_bytes)
                f.flush() # Ensure Python's buffer is flushed to OS
                os.fsync(f.fileno()) # Ensure OS buffer is flushed to disk
            logger.debug(f"Node {self.node_id}: Persisted log entry (term={entry.term}, size={len(entry_bytes)})")
        except IOError as e:
            logger.error(f"Node {self.node_id}: Failed to persist log entry to {self.log_file_path}: {e}", exc_info=True)
            # Critical error

    def _truncate_log_file(self, index: int):
        """Truncates the persisted log file *after* the entry at the given (0-based) index."""
        # This is complex to do safely and efficiently by modifying the file in place.
        # A simpler (but less efficient for large logs) approach is to reload the
        # in-memory log up to 'index' and rewrite the entire log file.
        logger.warning(f"Node {self.node_id}: Truncating log file from index {index+1} (0-based index {index})")
        try:
            # Keep entries up to and including 'index'
            entries_to_keep = self.log[:index + 1]
            temp_path = self.log_file_path + ".tmp"

            # Rewrite the log file with only the entries to keep
            with open(temp_path, 'wb') as f: # Write binary mode (overwrite)
                 for entry in entries_to_keep:
                      entry_bytes = entry.SerializeToString()
                      length_prefix = struct.pack("!I", len(entry_bytes))
                      f.write(length_prefix)
                      f.write(entry_bytes)
                 f.flush()
                 os.fsync(f.fileno())

            os.rename(temp_path, self.log_file_path)
            logger.info(f"Node {self.node_id}: Log file rewritten, truncated after index {index}")

        except IOError as e:
            logger.error(f"Node {self.node_id}: Failed to truncate log file at index {index}: {e}", exc_info=True)
            # Critical error - log file might be corrupt

    def _load_log(self):
        """Loads log entries from the log file."""
        self.log = []
        try:
            if not os.path.exists(self.log_file_path):
                logger.info(f"Node {self.node_id}: Log file not found. Starting with empty log.")
                # Create an empty file? Optional.
                # open(self.log_file_path, 'ab').close()
                return

            with open(self.log_file_path, 'rb') as f: # Read binary mode
                while True:
                    # Read length prefix (4 bytes)
                    length_bytes = f.read(4)
                    if not length_bytes:
                        break # End of file
                    if len(length_bytes) < 4:
                        logger.error(f"Node {self.node_id}: Corrupt log file? Could not read full length prefix.")
                        break # Treat as corrupt

                    entry_length = struct.unpack("!I", length_bytes)[0]

                    # Read the entry bytes
                    entry_bytes = f.read(entry_length)
                    if len(entry_bytes) < entry_length:
                        logger.error(f"Node {self.node_id}: Corrupt log file? Could not read full entry (expected {entry_length}, got {len(entry_bytes)}).")
                        # Decide whether to stop loading or try to continue
                        break # Stop loading on corruption

                    # Deserialize and append
                    entry = raft_pb2.LogEntry()
                    entry.ParseFromString(entry_bytes)
                    self.log.append(entry)

            logger.info(f"Node {self.node_id}: Loaded {len(self.log)} entries from log file.")

        except (IOError, struct.error, Exception) as e: # Catch protobuf parsing errors too
             logger.error(f"Node {self.node_id}: Failed to load log from {self.log_file_path}: {e}. Starting with potentially truncated log.", exc_info=True)
             # Log might be partially loaded or empty now due to error.

    # --- Core Raft Logic Methods (Placeholders) ---

    async def handle_request_vote(self, request: raft_pb2.RequestVoteArgs) -> raft_pb2.RequestVoteReply:
        logger.debug(f"Node {self.node_id}: Handling RequestVote from Candidate {request.candidate_id} for term {request.term}")

        # --- PERSISTENCE POINT ---
        # Need to persist self.current_term and self.voted_for before returning
        # We will add self._persist_state() calls later

        # 1. Reply false if term < currentTerm (§5.1)
        if request.term < self.current_term:
            logger.info(f"Node {self.node_id}: Denying vote to {request.candidate_id} (term {request.term} < current term {self.current_term})")
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)

        # If request term is higher, update term and become follower FIRST
        if request.term > self.current_term:
            logger.info(f"Node {self.node_id}: Received higher term {request.term} in RequestVote from {request.candidate_id}. Stepping down.")
            old_term = self.current_term # Keep old term for logging if needed
            self._become_follower(request.term)
            self._persist_state() # Persist updated term & reset voted_for

        # 2. If votedFor is null or candidateId... (§5.2, §5.4)
        vote_granted = False
        if self.voted_for is None or self.voted_for == request.candidate_id:
            # Check if candidate's log is at least as up-to-date as receiver's log (§5.4.1)
            my_last_log_index, my_last_log_term = self._get_last_log_info()
            candidate_last_log_index = request.last_log_index
            candidate_last_log_term = request.last_log_term

            candidate_log_is_ok = (
                candidate_last_log_term > my_last_log_term or
                (candidate_last_log_term == my_last_log_term and candidate_last_log_index >= my_last_log_index)
            )

            if candidate_log_is_ok:
                logger.info(f"Node {self.node_id}: Granting vote to Candidate {request.candidate_id} for term {self.current_term}. My log: (idx={my_last_log_index}, term={my_last_log_term}), Cand log: (idx={candidate_last_log_index}, term={candidate_last_log_term})")
                vote_granted = True
                self.voted_for = request.candidate_id # Record vote
                self._persist_state()
                self._reset_election_timer() # Reset timer ONLY if vote is granted
            else:
                logger.info(f"Node {self.node_id}: Denying vote to Candidate {request.candidate_id} for term {self.current_term}. Candidate log not up-to-date. My log: (idx={my_last_log_index}, term={my_last_log_term}), Cand log: (idx={candidate_last_log_index}, term={candidate_last_log_term})")
        else:
            logger.info(f"Node {self.node_id}: Denying vote to Candidate {request.candidate_id} (already voted for {self.voted_for} in term {self.current_term})")

        # Reply with current term (might have been updated) and vote status
        return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=vote_granted)

    async def handle_append_entries(self, request: raft_pb2.AppendEntriesArgs) -> raft_pb2.AppendEntriesReply:
        success = False # Initialize success to False
        logger.debug(f"Node {self.node_id}: Handling AppendEntries from {request.leader_id} for term {request.term}")

        # 1. Reply false if term < currentTerm (§5.1)
        if request.term < self.current_term:
            logger.info(f"Node {self.node_id}: Rejecting AppendEntries from {request.leader_id} (term {request.term} < current term {self.current_term})")
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)

        # Raft paper rules for AppendEntries (§5.1, §5.3):
        # 1. Reply false if term < currentTerm (handled above)
        # 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        # 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
        # 4. Append any new entries not already in the log
        # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        # If RPC received: reset election timer. If term > currentTerm: step down.

        if request.term > self.current_term:
            logger.info(f"Node {self.node_id}: Received higher term {request.term} from leader {request.leader_id}. Stepping down.")
            self._become_follower(request.term)
            self._persist_state() # Persist updated term & reset voted_for
            # Continue processing the AppendEntries in the new term below

        # (Only process if term is current or we just updated it)
        if request.term == self.current_term:
            # Reset timer if we are follower/candidate or stepping down
            if self.state != NodeState.LEADER:
                self._become_follower(request.term) # Ensure follower state if candidate/leader sees valid leader
                self.leader_id = request.leader_id
                logger.debug(f"Node {self.node_id}: Recognized {self.leader_id} as leader for term {self.current_term}")
                self._reset_election_timer()

            # --- Log Consistency Check (§5.3) ---
            log_matched = False # Initialize here

            # 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if request.prev_log_index == 0:
                # If prevLogIndex is 0, it's effectively a match with the "entry" before the first log entry
                log_matched = True
            elif request.prev_log_index > len(self.log):
                # Log doesn't contain prevLogIndex (it's too short)
                logger.info(f"Node {self.node_id}: Rejecting AppendEntries. Log too short (len={len(self.log)}, prevLogIndex={request.prev_log_index})")
                # log_matched remains False
            elif self.log[request.prev_log_index - 1].term != request.prev_log_term:
                # Term at (0-indexed) self.log[request.prev_log_index - 1] does not match prevLogTerm
                logger.info(f"Node {self.node_id}: Rejecting AppendEntries. Term mismatch at index {request.prev_log_index} (myTerm={self.log[request.prev_log_index - 1].term}, prevLogTerm={request.prev_log_term})")
                # log_matched remains False
                # TODO: Optimization: Could include conflict term/index in reply to help leader find match faster
            else:
                # Log is long enough and term at prevLogIndex matches.
                log_matched = True

            if log_matched:
                success = True # Set success to True ONLY if log_matched is confirmed
                # --- Append/Truncate Logic ---
                current_log_index = request.prev_log_index # Start checking from the entry *after* prevLogIndex
                for i, entry in enumerate(request.entries):
                    current_log_index += 1
                    if current_log_index > len(self.log):
                        # Append new entry
                        self.log.append(entry)
                        self._persist_log_entry(entry) # Persist *before* adding to in-memory? Or after? Raft needs it durable. Let's persist first.
                        logger.debug(f"Node {self.node_id}: Appended entry at index {current_log_index} (term {entry.term})")
                    elif self.log[current_log_index - 1].term != entry.term:
                        # Conflict: delete existing entry and all that follow
                        logger.warning(f"Node {self.node_id}: Log conflict at index {current_log_index}. Truncating log. Existing term: {self.log[current_log_index - 1].term}, New term: {entry.term}")
                        # Truncate in-memory log FIRST
                        self.log = self.log[:current_log_index - 1]
                        # Truncate persisted log file
                        self._truncate_log_file(current_log_index - 2) # Pass 0-based index of last entry to keep

                        # Append the new entry
                        self.log.append(entry)
                        self._persist_log_entry(entry)
                        logger.debug(f"Node {self.node_id}: Appended entry at index {current_log_index} (term {entry.term}) after truncation")
                    # else: entry matches, do nothing

                # --- Update Commit Index ---
                if request.leader_commit > self.commit_index:
                    # commitIndex = min(leaderCommit, index of last new entry)
                    # 'index of last new entry' is the index of the last entry processed in the loop above
                    new_commit_index = min(request.leader_commit, current_log_index)
                    if new_commit_index > self.commit_index:
                        self.commit_index = new_commit_index
                        logger.info(f"Node {self.node_id}: Updated commitIndex to {self.commit_index}")
                        # --- Trigger State Machine Application ---
                        self._apply_log_entries() # We'll add this later

        # Reply section remains largely the same, term might have updated
        return raft_pb2.AppendEntriesReply(term=self.current_term, success=success)

        # Other methods (like becoming follower/candidate/leader) don't
        # inherently require persistence *at that moment*, they rely on the
        # state being persisted when it changes (term/vote) or when log entries
        # are added.

    async def handle_client_command(self, request: raft_pb2.ClientCommandRequest) -> raft_pb2.ClientCommandReply:
            logger.debug(f"Node {self.node_id}: Handling Client Command (ID: {request.command_id})")

            # 1. Check if leader
            if self.state != NodeState.LEADER:
                leader_hint = self.peers_addresses.get(self.leader_id, "")
                logger.info(f"Node {self.node_id}: Not leader, redirecting client to {leader_hint}")
                return raft_pb2.ClientCommandReply(success=False, leader_hint=leader_hint, message="Not the leader")

            # --- Try parsing command locally first for GET ---
            parsed_command = self._parse_command(request.command)
            operation = None
            key = None
            if parsed_command:
                operation, key, _ = parsed_command # Ignore value for now

            # --- Handle GET directly (Linearizability Concern: Read from Leader's state) ---
            if operation == "GET" and key is not None:
                value = self.kv_store.get(key, None) # Read from local KV store
                if value is not None:
                    logger.info(f"Node {self.node_id}: Leader handled GET for key '{key}' (found)")
                    return raft_pb2.ClientCommandReply(
                        success=True,
                        message="Key found",
                        value=value
                    )
                else:
                    logger.info(f"Node {self.node_id}: Leader handled GET for key '{key}' (not found)")
                    return raft_pb2.ClientCommandReply(success=False, message="Key not found")
            # --- END GET Handling ---

            # --- Handle PUT/DELETE (go through Raft log) ---
            elif operation == "PUT" or operation == "DELETE":
                logger.info(f"Node {self.node_id}: Leader received {operation} command for key '{key}'. Appending to log.")
                # Proceed with appending to log and waiting for commit...
                log_entry = raft_pb2.LogEntry(
                    term=self.current_term,
                    command=request.command
                )
                self.log.append(log_entry)
                log_index = len(self.log)
                self._persist_log_entry(log_entry)
                logger.info(f"Node {self.node_id}: Leader appended {operation} command to log at index {log_index}, term {self.current_term}")

                commit_future = asyncio.Future()
                async with self._commit_futures_lock:
                    self._commit_futures[log_index] = commit_future

                try:
                    client_timeout_seconds = 10.0
                    logger.debug(f"Node {self.node_id}: Leader waiting for commit of index {log_index}...")
                    await asyncio.wait_for(commit_future, timeout=client_timeout_seconds)

                    # Apply happens automatically now via _apply_log_entries triggered by commit index update

                    logger.info(f"Node {self.node_id}: Leader confirmed commit of index {log_index}. Replying success to client.")
                    return raft_pb2.ClientCommandReply(success=True, message=f"{operation} command committed")

                except asyncio.TimeoutError:
                    logger.error(f"Node {self.node_id}: Timeout waiting for commit of log index {log_index} for client command {request.command_id}")
                    async with self._commit_futures_lock:
                        if log_index in self._commit_futures and not self._commit_futures[log_index].done():
                            self._commit_futures[log_index].cancel("Client request timed out")
                        self._commit_futures.pop(log_index, None)
                    return raft_pb2.ClientCommandReply(success=False, message="Timeout waiting for command commit")
                except Exception as e:
                    logger.error(f"Node {self.node_id}: Error processing client command {request.command_id} for index {log_index}: {e}", exc_info=True)
                    async with self._commit_futures_lock:
                        if log_index in self._commit_futures and not self._commit_futures[log_index].done():
                            self._commit_futures[log_index].set_exception(e)
                        self._commit_futures.pop(log_index, None)
                    return raft_pb2.ClientCommandReply(success=False, message=f"Internal server error: {e}")
            else:
                # Command was not parsable or not PUT/GET/DELETE
                logger.error(f"Node {self.node_id}: Leader received invalid/unsupported client command: {request.command[:50]}...")
                return raft_pb2.ClientCommandReply(success=False, message="Invalid or unsupported command")

    # --- State Transitions ---

    def _become_follower(self, term: int):
        """Transitions node to Follower state."""
        if self.state != NodeState.FOLLOWER or term > self.current_term:
             logger.info(f"Node {self.node_id}: Transitioning to Follower state for term {term}")
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None # Reset vote for the new term
        self.leader_id = None # Reset leader hint
        self._cancel_heartbeat_timer() # Followers don't send heartbeats
        self._reset_election_timer() # Start election timer

    def _become_candidate(self):
        """Transitions node to Candidate state and starts election."""
        if self.state == NodeState.LEADER:
            logger.warn(f"Node {self.node_id}: Leader attempting to become candidate? Ignoring.")
            return

        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id # Vote for self
        self._votes_received = {self.node_id} # Count self vote
        self.leader_id = None

        # --- PERSIST STATE CHANGE ---
        self._persist_state()

        logger.info(f"Node {self.node_id}: Transitioning to Candidate state for term {self.current_term}. Voted for self.")

        self._reset_election_timer() # Reset timer to start election period
        self._cancel_heartbeat_timer()

        # --- Start Election --- Implementation needed
        # Send RequestVote RPCs to all other peers concurrently
        asyncio.create_task(self._start_election())

    def _become_leader(self):
            """Transitions node to Leader state."""
            if self.state != NodeState.CANDIDATE:
                logger.error(f"Node {self.node_id}: Invalid transition to Leader from {self.state}. Ignoring.")
                return

            logger.info(f"Node {self.node_id}: Transitioning to Leader state for term {self.current_term}")
            self.state = NodeState.LEADER
            self.leader_id = self.node_id # I am the leader
            self._cancel_election_timer() # Leaders don't need election timers

            # Initialize leader-specific state
            last_log_idx = len(self.log)
            self.next_index = {peer_id: last_log_idx + 1 for peer_id in self.peers_addresses} # Initialize for ALL peers (including self for consistency)
            self.match_index = {peer_id: 0 for peer_id in self.peers_addresses} # Initialize for ALL peers

            # --- Add No-Op Entry for Current Term ---
            logger.info(f"Node {self.node_id}: Leader adding initial no-op entry for term {self.current_term}")
            no_op_entry = raft_pb2.LogEntry(term=self.current_term, command=b'NOOP') # Use a special command string or empty bytes
            self.log.append(no_op_entry)
            self._persist_log_entry(no_op_entry)
            # Leader immediately matches its own new entry
            self.match_index[self.node_id] = len(self.log)
            self.next_index[self.node_id] = len(self.log) + 1
            logger.debug(f"Node {self.node_id}: Appended NOOP entry at index {len(self.log)}, term {self.current_term}")
            # --- End No-Op ---

            # Start sending heartbeats immediately (which will now include the no-op)
            self._start_heartbeat_timer() # This will trigger _send_append_entries_to_all

            # Optional: Trigger an immediate replication cycle instead of waiting for the timer
            # asyncio.create_task(self._send_append_entries_to_all())


    # --- Election Logic ---

    async def _start_election(self):
        """Sends RequestVote RPCs to peers."""
        if self.state != NodeState.CANDIDATE:
            return

        majority = (len(self.peers_addresses) // 2) + 1
        logger.info(f"Node {self.node_id}: Starting election for term {self.current_term}. Need {majority} votes.")

        # Get actual last log index/term
        last_log_index, last_log_term = self._get_last_log_info()

        args = raft_pb2.RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )

        # Reset votes received for this election attempt
        self._votes_received = {self.node_id} # Automatically count self-vote

        # Create tasks to send RPCs concurrently
        vote_tasks = []
        for peer_id, peer_addr in self.peers_addresses.items():
            if peer_id != self.node_id:
                # Pass args by value (copy implicitly)
                vote_tasks.append(asyncio.create_task(self._send_request_vote(peer_id, peer_addr, args)))

        # Process results as they complete (more efficient than waiting for all)
        # We use asyncio.as_completed to process results immediately
        pending_tasks = set(vote_tasks)
        while pending_tasks and self.state == NodeState.CANDIDATE and args.term == self.current_term:
            done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                result = task.result() # Get the result (peer_id, reply) or None/Exception

                if result is None: # Handle RPC errors or None replies
                     continue

                peer_id, reply = result # Unpack the result

                if reply.term > self.current_term:
                    logger.info(f"Node {self.node_id}: Discovered higher term {reply.term} from vote reply from {peer_id}. Stepping down.")
                    self._become_follower(reply.term)
                    self._persist_state()
                    # Cancel remaining vote tasks (optional but cleaner)
                    for p_task in pending_tasks:
                        p_task.cancel()
                    return # Stop election processing

                if reply.vote_granted:
                    logger.debug(f"Node {self.node_id}: Vote granted by peer {peer_id} for term {self.current_term}")
                    self._votes_received.add(peer_id) # Add the ID of the granting peer
                    # Check if majority reached
                    if len(self._votes_received) >= majority:
                        logger.info(f"Node {self.node_id}: Received majority votes ({len(self._votes_received)}/{majority}). Becoming Leader.")
                        self._become_leader()
                        # Cancel remaining vote tasks (optional but cleaner)
                        for p_task in pending_tasks:
                            p_task.cancel()
                        return # Election successful

        # If loop finishes and still candidate, election failed or was superseded
        if self.state == NodeState.CANDIDATE and args.term == self.current_term:
             logger.info(f"Node {self.node_id}: Election finished for term {self.current_term}, did not receive majority or state changed. Votes: {len(self._votes_received)}/{majority}")
        # Cleanup any remaining pending tasks if loop exited early
        for p_task in pending_tasks:
            p_task.cancel()

    async def _send_request_vote(self, peer_id: int, peer_addr: str, args: raft_pb2.RequestVoteArgs) -> Optional[Tuple[int, raft_pb2.RequestVoteReply]]:
        """Helper to send a single RequestVote RPC. Returns (peer_id, reply) on success."""
        try:
            stub = await self._get_peer_stub(peer_id, peer_addr)
            if stub:
                 request_timeout = (ELECTION_TIMEOUT_MIN_MS / 1000.0) * 0.8
                 logger.debug(f"Node {self.node_id}: Sending RequestVote term {args.term} to {peer_id} with timeout {request_timeout:.2f}s")
                 reply = await stub.RequestVote(args, timeout=request_timeout)
                 logger.debug(f"Node {self.node_id}: Received RequestVote reply from {peer_id}: term={reply.term}, granted={reply.vote_granted}")
                 return (peer_id, reply) # Return tuple with peer_id
            else:
                 logger.warning(f"Node {self.node_id}: Could not get stub for peer {peer_id}")
                 return None
        except grpc.aio.AioRpcError as e:
             logger.warning(f"Node {self.node_id}: RPC error sending RequestVote to {peer_id} ({peer_addr}): {e.code()} - {e.details()}")
             return None
        except asyncio.TimeoutError: # Catch asyncio timeout specifically if grpc doesn't map it
             logger.warning(f"Node {self.node_id}: RPC timeout sending RequestVote to {peer_id} ({peer_addr})")
             return None
        except Exception as e:
            logger.error(f"Node {self.node_id}: Unexpected error sending RequestVote to {peer_id}: {e}", exc_info=True)
            return None


    # --- Timer Management ---

    def _reset_election_timer(self):
        """Resets the election timer to a new random interval."""
        self._cancel_election_timer() # Cancel any existing timer
        timeout = random.uniform(ELECTION_TIMEOUT_MIN_MS / 1000.0, ELECTION_TIMEOUT_MAX_MS / 1000.0)

        # Get the currently running loop HERE
        loop = asyncio.get_running_loop()
        self._election_timer = loop.call_later(timeout, self._election_timeout)
        # Note: self._election_timeout doesn't need to be async,
        # call_later just schedules it to run in the loop context.

        logger.debug(f"Node {self.node_id}: Election timer reset to {timeout:.3f} seconds.")

    #helper function - out of place.
    async def _notify_commit_futures(self, committed_up_to_index: int):
        """Notify client futures whose log entries are now committed."""
        async with self._commit_futures_lock:
            # Iterate over a copy of keys since we might modify the dict
            indices_to_notify = list(self._commit_futures.keys())
            for index in indices_to_notify:
                if index <= committed_up_to_index:
                    future = self._commit_futures.pop(index, None) # Remove and get future
                    if future and not future.done():
                        logger.debug(f"Node {self.node_id}: Notifying future for committed index {index}")
                        future.set_result(True) # Signal success
                    elif future and future.done():
                        # Already done (e.g., timed out/cancelled), log maybe?
                        logger.debug(f"Node {self.node_id}: Future for committed index {index} was already done.")

    def _parse_command(self, command_bytes: bytes) -> Optional[Tuple[str, str, bytes]]:
            """
            Parses the command bytes from a log entry.
            Format: OPERATION\nKEY\nVALUE (value is empty for GET/DEL)
            Returns (operation, key, value) or None if invalid.
            Value is only present for PUT.
            """
            # Use the same separator as the client
            COMMAND_SEPARATOR = b'\n' # Define here or import if shared

            try:
                # Split based on newline separator
                parts = command_bytes.split(COMMAND_SEPARATOR, 2) # <<<--- CHANGE HERE
                operation = parts[0].decode('utf-8').upper()

                if operation == "PUT" and len(parts) == 3:
                    key = parts[1].decode('utf-8')
                    # Ensure key doesn't contain separator itself (already checked client-side, but good practice)
                    if COMMAND_SEPARATOR in parts[1]:
                        logger.warning(f"Node {self.node_id}: Separator found in key during parsing: {parts[1][:50]}...")
                        return None
                    value = parts[2] # Keep value as bytes
                    return operation, key, value
                elif (operation == "GET" or operation == "DELETE") and len(parts) >= 2: # Allow parts=3 if extra newline was sent?
                    # Check length >= 2, allow for potential trailing newline on key?
                    key = parts[1].decode('utf-8')
                    if COMMAND_SEPARATOR in parts[1]:
                        logger.warning(f"Node {self.node_id}: Separator found in key during parsing: {parts[1][:50]}...")
                        return None
                    return operation, key, b'' # Return empty bytes for value
                else:
                    logger.warning(f"Node {self.node_id}: Could not parse command - incorrect parts for op '{operation}': {len(parts)} parts found. Command bytes: {command_bytes[:100]}...") # Log more bytes
                    return None
            except Exception as e:
                logger.error(f"Node {self.node_id}: Error parsing command '{command_bytes[:100]}...': {e}", exc_info=True)
                return None

    def _apply_log_entries(self):
            """Applies committed log entries to the state machine (KV store)."""
            # Apply entries from last_applied + 1 up to commit_index
            # Important: Do this carefully, ensure indices are handled correctly (0-based vs 1-based)

            # Loop while the last applied index (0-based) is less than the committed index (1-based)
            while self.last_applied < self.commit_index:
                apply_idx_0based = self.last_applied # 0-based index to apply

                # Safety check: Ensure the entry exists in the log
                if apply_idx_0based >= len(self.log):
                    # This should ideally not happen if commit_index logic is correct
                    logger.error(f"Node {self.node_id}: Cannot apply index {apply_idx_0based + 1}, log length is {len(self.log)}. Stopping application.")
                    break # Stop processing if we try to apply beyond the log end

                entry_to_apply = self.log[apply_idx_0based]
                command_bytes = entry_to_apply.command
                log_index = apply_idx_0based + 1 # 1-based log index for logging

                # --- Handle NOOP entries first ---
                if command_bytes == b'NOOP':
                    # Log the application of the NOOP entry
                    logger.info(f"Node {self.node_id}: Applying NOOP log index {log_index} (term {entry_to_apply.term})")
                    # No state change is needed for a NOOP entry

                # --- Handle regular client commands ---
                else:
                    # Log that we are applying a regular command
                    logger.info(f"Node {self.node_id}: Applying log index {log_index} (term {entry_to_apply.term}) to state machine.")

                    # Parse the command bytes
                    parsed_command = self._parse_command(command_bytes)

                    if parsed_command:
                        operation, key, value = parsed_command
                        if operation == "PUT":
                            self.kv_store[key] = value
                            logger.debug(f"Node {self.node_id}: Applied PUT key='{key}' (value size={len(value)})")
                        elif operation == "DELETE":
                            # Use pop with default None to avoid KeyError if key doesn't exist
                            old_value = self.kv_store.pop(key, None)
                            if old_value is not None:
                                logger.debug(f"Node {self.node_id}: Applied DELETE key='{key}' (existed)")
                            else:
                                logger.debug(f"Node {self.node_id}: Applied DELETE key='{key}' (did not exist)")
                        elif operation == "GET":
                            # GET operations don't modify state, applying them is a no-op here.
                            # They are handled directly by client requests checking the state machine later.
                            # Log for completeness.
                            logger.debug(f"Node {self.node_id}: Applying GET key='{key}' (no state change during apply phase)")
                        else:
                            # Should not happen if parsing logic covers all valid command types from format_command
                            logger.warning(f"Node {self.node_id}: Unknown operation '{operation}' encountered during apply for index {log_index}")
                    else:
                        # Log an error if parsing failed for a non-NOOP command
                        logger.error(f"Node {self.node_id}: Failed to parse command for applying log index {log_index}. Skipping application for this entry. Command bytes: {command_bytes[:100]}...")

                # --- Successfully applied entry (or handled NOOP/error), advance last_applied ---
                self.last_applied += 1

            # Optional: Log after the loop finishes if any entries were applied
            if self.last_applied > 0: # Check if any entries have ever been applied
                # Can compare with initial value if needed, but this simple check is fine
                logger.debug(f"Node {self.node_id}: Finished applying entries loop. lastApplied index reached = {self.last_applied} (commit_index = {self.commit_index})")

            # TODO: Persist last_applied? If the node crashes, it needs to know
            #       where it left off applying. For simplicity, we are not persisting
            #       last_applied or the kv_store itself in this version.
            #       A real system would need to persist the kv_store or snapshot it,
            #       and persist last_applied.



    def _cancel_election_timer(self):
        """Cancels the election timer if it's running."""
        if self._election_timer:
            self._election_timer.cancel()
            self._election_timer = None

    def _election_timeout(self):
        """Callback function when the election timer expires."""
        if self.state == NodeState.FOLLOWER or self.state == NodeState.CANDIDATE:
            logger.info(f"Node {self.node_id}: Election timeout reached in state {self.state}. Starting election.")
            self._become_candidate()
        else: # Leader state
            logger.debug(f"Node {self.node_id}: Election timeout callback triggered in Leader state (should be cancelled). Ignoring.")
            # This might happen due to race conditions if timer fired just as node became leader

    def _start_heartbeat_timer(self):
        """Starts the periodic heartbeat task for Leaders."""
        self._cancel_heartbeat_timer() # Ensure only one runs
        if self.state == NodeState.LEADER:
            logger.info(f"Node {self.node_id}: Starting heartbeat timer (interval: {HEARTBEAT_INTERVAL_MS}ms)")
            self._heartbeat_timer = asyncio.create_task(self._send_heartbeats_periodically())
        else:
             logger.warning(f"Node {self.node_id}: Tried to start heartbeat timer when not Leader.")


    def _cancel_heartbeat_timer(self):
        """Cancels the heartbeat task if it's running."""
        if self._heartbeat_timer and not self._heartbeat_timer.done():
            self._heartbeat_timer.cancel()
            logger.debug(f"Node {self.node_id}: Heartbeat timer cancelled.")
        self._heartbeat_timer = None

    async def _send_heartbeats_periodically(self):
        """Task that periodically sends AppendEntries (heartbeats)."""
        while self.state == NodeState.LEADER:
            try:
                logger.debug(f"Node {self.node_id}: Sending heartbeats for term {self.current_term}")
                # --- Send AppendEntries (empty) to all peers --- Implementation needed
                # Use asyncio.gather to send concurrently
                # Process replies: if term > currentTerm, step down
                await self._send_append_entries_to_all() # TODO: Create this helper method

                # Wait for the next interval
                await asyncio.sleep(HEARTBEAT_INTERVAL_MS / 1000.0)
            except asyncio.CancelledError:
                logger.info(f"Node {self.node_id}: Heartbeat task cancelled.")
                break # Exit loop if task is cancelled
            except Exception as e:
                logger.error(f"Node {self.node_id}: Error in heartbeat loop: {e}", exc_info=True)
                # Avoid busy-looping on error, wait a bit before retrying
                await asyncio.sleep(1)

    async def _send_append_entries_to_all(self):
            """Sends AppendEntries (potentially with log entries) to all peers."""
            if self.state != NodeState.LEADER:
                logger.debug(f"Node {self.node_id}: Not leader, skipping AppendEntries")
                return

            # Track the term we started with to detect term changes
            start_term = self.current_term
            tasks = []

            # Prepare requests for all peers
            for peer_id, peer_addr in self.peers_addresses.items():
                if peer_id == self.node_id:
                    continue

                # Get log state for this peer with bounds checking
                next_idx = min(self.next_index.get(peer_id, 1), len(self.log) + 1)
                prev_log_index = next_idx - 1
                prev_log_term = 0

                if prev_log_index > 0 and prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1].term

                # Determine entries to send with batch size limit
                MAX_ENTRIES_PER_BATCH = 100  # Configurable
                entries_to_send = self.log[next_idx - 1:next_idx - 1 + MAX_ENTRIES_PER_BATCH]

                logger.debug(f"Node {self.node_id}: Preparing AppendEntries for {peer_id}: "
                           f"nextIndex={next_idx}, prevLogIndex={prev_log_index}, "
                           f"prevLogTerm={prev_log_term}, numEntries={len(entries_to_send)}")

                args = raft_pb2.AppendEntriesArgs(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries_to_send,
                    leader_commit=self.commit_index
                )
                tasks.append(asyncio.create_task(self._send_single_append_entries(peer_id, peer_addr, args)))

            # Process results as they arrive instead of waiting for all
            pending = tasks
            while pending and self.state == NodeState.LEADER and self.current_term == start_term:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                for task in done:
                    try:
                        result = task.result()
                        if not result:
                            continue

                        peer_id, reply, request_args = result

                        # Check if we're still leader and in the same term
                        if self.state != NodeState.LEADER or self.current_term != start_term:
                            logger.info(f"Node {self.node_id}: No longer leader or term changed, stopping AppendEntries processing")
                            return

                        if reply.term > self.current_term:
                            logger.info(f"Node {self.node_id}: Discovered higher term {reply.term} from {peer_id}, stepping down")
                            self._become_follower(reply.term)
                            self._persist_state()
                            return

                        # Process successful responses
                        if reply.success:
                            new_match_index = request_args.prev_log_index + len(request_args.entries)
                            new_next_index = new_match_index + 1

                            # Update indices atomically
                            self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), new_match_index)
                            self.next_index[peer_id] = max(self.next_index.get(peer_id, 1), new_next_index)

                            logger.debug(f"Node {self.node_id}: Updated indices for {peer_id}: "
                                       f"match_index={self.match_index[peer_id]}, "
                                       f"next_index={self.next_index[peer_id]}")
                        else:
                            # On failure, decrease next_index but ensure it doesn't go below 1
                            # Also implement fast backup using conflict index if provided
                            current_next = self.next_index.get(peer_id, 1)
                            new_next = max(1, current_next - 1)
                            self.next_index[peer_id] = new_next
                            logger.debug(f"Node {self.node_id}: Decreased next_index for {peer_id} to {new_next}")

                    except Exception as e:
                        logger.error(f"Node {self.node_id}: Error processing AppendEntries result: {e}", exc_info=True)

                # Update commit index if we're still leader
                if self.state == NodeState.LEADER and self.current_term == start_term:
                    await self._update_commit_index()

            # Clean up any remaining tasks if we exited the loop early
            for task in pending:
                task.cancel()
                self.commit_index = N
                logger.info(f"Node {self.node_id}: Leader updated commitIndex from {initial_commit_index_before_update} to {self.commit_index} based on majority match ({match_count}/{majority})")
                # Trigger commit notification for waiting clients
                asyncio.create_task(self._notify_commit_futures(self.commit_index))
                # Trigger state machine application
    async def _update_commit_index(self):
        """Helper method to update the leader's commit index based on match_index values."""
        if self.state != NodeState.LEADER:
            return

        majority = (len(self.peers_addresses) // 2) + 1
        match_indices = sorted([self.match_index.get(peer_id, 0)
                              for peer_id in self.peers_addresses.keys()])

        # The median of match_indices is guaranteed to be committed if it's from current term
        potential_commit = match_indices[majority - 1]  # 0-based indexing

        # Only update if the potential commit index is greater than current
        if potential_commit > self.commit_index:
            # Verify the entry at potential_commit is from current term
            if potential_commit <= len(self.log) and self.log[potential_commit - 1].term == self.current_term:
                old_commit = self.commit_index
                self.commit_index = potential_commit
                logger.info(f"Node {self.node_id}: Advanced commit index {old_commit} -> {self.commit_index}")

                # Notify clients and apply entries
                await self._notify_commit_futures(self.commit_index)
                self._apply_log_entries()

    async def _send_single_append_entries(self, peer_id: int, peer_addr: str, args: raft_pb2.AppendEntriesArgs) -> Optional[Tuple[int, raft_pb2.AppendEntriesReply, raft_pb2.AppendEntriesArgs]]:
        """
        Helper to send a single AppendEntries RPC with improved error handling and retry logic.
        Returns (peer_id, reply, original_args) on success, None on failure.

        The function implements:
        1. Dynamic timeout based on message size
        2. Better error handling and logging
        3. Stub connection management
        4. Proper resource cleanup
        """
        # Calculate timeout based on message size and type
        base_timeout = HEARTBEAT_INTERVAL_MS / 1000.0
        entries_size = sum(len(entry.SerializeToString()) for entry in args.entries)
        # Add more time for larger payloads, but cap it
        size_factor = min(2.0, 1.0 + (entries_size / (1024 * 1024)))  # Scale up to 2x for 1MB+
        request_timeout = min(
            base_timeout * size_factor * 2.0,  # Double the heartbeat interval as base
            ELECTION_TIMEOUT_MIN_MS / 1000.0 * 0.8  # But never exceed 80% of min election timeout
        )

        retry_count = 0
        MAX_RETRIES = 2  # Limit retries to avoid infinite loops
        BACKOFF_BASE = 0.1  # 100ms base backoff

        while retry_count <= MAX_RETRIES:
            try:
                # Get or create stub with connection check
                stub = None
                try:
                    stub = await self._get_peer_stub(peer_id, peer_addr)
                    if not stub:
                        logger.error(f"Node {self.node_id}: Failed to get/create stub for peer {peer_id} at {peer_addr}")
                        return None
                except Exception as e:
                    logger.error(f"Node {self.node_id}: Error creating stub for peer {peer_id}: {e}", exc_info=True)
                    return None

                # Log attempt details
                entries_count = len(args.entries)
                logger.debug(
                    f"Node {self.node_id}: Sending AppendEntries to {peer_id} "
                    f"(term={args.term}, entries={entries_count}, "
                    f"prevIndex={args.prev_log_index}, timeout={request_timeout:.2f}s)"
                )

                # Use asyncio.wait_for for more reliable timeout handling
                try:
                    reply = await asyncio.wait_for(
                        stub.AppendEntries(args),
                        timeout=request_timeout
                    )

                    # Successful RPC, check reply
                    if not reply:
                        logger.warning(f"Node {self.node_id}: Received empty reply from {peer_id}")
                        return None

                    # Log success details
                    logger.debug(
                        f"Node {self.node_id}: Received AppendEntries reply from {peer_id}: "
                        f"term={reply.term}, success={reply.success}"
                    )

                    return (peer_id, reply, args)

                except asyncio.TimeoutError:
                    logger.warning(
                        f"Node {self.node_id}: Timeout sending AppendEntries to {peer_id} "
                        f"(attempt {retry_count + 1}/{MAX_RETRIES + 1}, timeout={request_timeout:.2f}s)"
                    )
                    # Don't retry on timeout - it's usually better to let the next heartbeat handle it
                    return None

            except grpc.aio.AioRpcError as e:
                # Handle specific gRPC errors
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.warning(
                        f"Node {self.node_id}: Peer {peer_id} unavailable "
                        f"(attempt {retry_count + 1}/{MAX_RETRIES + 1}): {e.details()}"
                    )
                    # Invalidate stub on connection errors
                    self.peer_stubs.pop(peer_id, None)
                elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logger.warning(
                        f"Node {self.node_id}: RPC deadline exceeded for peer {peer_id} "
                        f"(attempt {retry_count + 1}/{MAX_RETRIES + 1})"
                    )
                else:
                    logger.error(
                        f"Node {self.node_id}: Unexpected gRPC error with peer {peer_id}: "
                        f"{e.code()} - {e.details()}"
                    )
                    # Don't retry on unexpected errors
                    return None

            except Exception as e:
                logger.error(
                    f"Node {self.node_id}: Unexpected error sending AppendEntries to {peer_id}: {e}",
                    exc_info=True
                )
                # Don't retry on unexpected errors
                return None

            # Implement exponential backoff for retries
            retry_count += 1
            if retry_count <= MAX_RETRIES:
                backoff = BACKOFF_BASE * (2 ** (retry_count - 1))  # Exponential backoff
                backoff = min(backoff, HEARTBEAT_INTERVAL_MS / 1000.0)  # Cap at heartbeat interval
                await asyncio.sleep(backoff)

        # If we get here, we've exhausted retries
        logger.error(f"Node {self.node_id}: Failed to send AppendEntries to {peer_id} after {MAX_RETRIES + 1} attempts")
        return None

    # --- gRPC Client Stub Management ---

    async def _get_peer_stub(self, peer_id: int, peer_addr: str) -> Optional[raft_pb2_grpc.RaftServiceStub]:
        """Gets or creates a gRPC client stub for a peer."""
        if peer_id not in self.peer_stubs:
            try:
                # Create an insecure channel for simplicity
                # For production, use secure channels (grpc.aio.secure_channel)
                channel = grpc.aio.insecure_channel(peer_addr)
                # Optional: Add connection testing or readiness check here?
                # await channel.channel_ready() # Could block or fail
                self.peer_stubs[peer_id] = raft_pb2_grpc.RaftServiceStub(channel)
                logger.debug(f"Node {self.node_id}: Created gRPC stub for peer {peer_id} at {peer_addr}")
            except Exception as e:
                logger.error(f"Node {self.node_id}: Failed to create channel/stub for peer {peer_id} at {peer_addr}: {e}", exc_info=True)
                return None
        return self.peer_stubs[peer_id]

    # --- Lifecycle Methods ---

    async def run(self):
        """Starts the node's background tasks (timers)."""
        logger.info(f"Node {self.node_id}: Starting main loop.")
        # Initialize peer stubs (could be done lazily too)
        # for pid, addr in self.peers_addresses.items():
        #     if pid != self.node_id:
        #         await self._get_peer_stub(pid, addr) # Pre-connect? Maybe not best idea.

        # Start as follower, which resets the election timer
        self._become_follower(self.current_term)

        # Keep running indefinitely (or until stop signal)
        # The actual work happens in timer callbacks and RPC handlers
        while True:
             # We could add periodic checks or tasks here if needed
             await asyncio.sleep(3600) # Sleep for a long time, woken by signals/timers

    async def stop(self):
        """Cleans up resources."""
        logger.info(f"Node {self.node_id}: Stopping...")
        self._cancel_election_timer()
        self._cancel_heartbeat_timer()
        # Close gRPC client channels
        for peer_id, stub in self.peer_stubs.items():
            if hasattr(stub, '_channel'): # Access underlying channel if possible
                try:
                    # Ensure channel is closed
                    await stub._channel.close() # Close the channel gracefully
                    logger.debug(f"Node {self.node_id}: Closed gRPC channel to peer {peer_id}")
                except Exception as e:
                    logger.warning(f"Node {self.node_id}: Error closing channel to peer {peer_id}: {e}")
        self.peer_stubs.clear()
        logger.info(f"Node {self.node_id}: Stopped.")

    def _get_last_log_info(self) -> Tuple[int, int]:
        """Returns the index and term of the last entry in the log."""
        # Log index is 1-based in Raft paper, len(log) is 0-based index + 1
        last_log_index = len(self.log)
        last_log_term = 0
        if self.log:
            last_log_term = self.log[-1].term
        return last_log_index, last_log_term
