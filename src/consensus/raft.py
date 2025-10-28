# src/consensus/raft.py
import asyncio
import random
import json
import time
from loguru import logger
from aiohttp import web
from src.communication.message_passing import send_json
from src.utils.config import NODE_ID, CLUSTER_ADDRS
from src.utils.redis_client import get_redis


class LogEntry:
    """Represents a single log entry in Raft"""
    def __init__(self, term: int, command: dict, index: int):
        self.term = term
        self.command = command
        self.index = index
    
    def to_dict(self):
        return {
            "term": self.term,
            "command": self.command,
            "index": self.index
        }
    
    @staticmethod
    def from_dict(data):
        return LogEntry(data["term"], data["command"], data["index"])


class RaftNode:
    """
    Complete Raft Consensus implementation with:
    - Leader election
    - Log replication
    - Persistence (via Redis)
    - Safety guarantees
    """
    
    def __init__(self, node_id=NODE_ID, cluster_addrs=CLUSTER_ADDRS):
        self.node_id = node_id
        self.cluster_addrs = cluster_addrs
        
        # Persistent state (must be saved to Redis before responding to RPCs)
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of LogEntry objects
        
        # Volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0
        
        # Volatile state on leaders (reinitialized after election)
        self.next_index = {}   # for each server, index of next log entry to send
        self.match_index = {}  # for each server, index of highest log entry known to be replicated
        
        # State management
        self.state = "follower"  # follower | candidate | leader
        self.leader_id = None
        
        # Events and timers
        self.heartbeat_received = asyncio.Event()
        self.running = False
        self.election_timeout = self._random_election_timeout()
        self.heartbeat_interval = 1.0  # 1 second
        
        # Metrics
        self.election_count = 0
        self.heartbeat_count = 0

    # =========================
    # PERSISTENCE (Redis)
    # =========================
    async def _save_state(self):
        """Save persistent state to Redis"""
        try:
            redis = await get_redis()
            state = {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "log": [entry.to_dict() for entry in self.log]
            }
            await redis.set(f"raft:state:{self.node_id}", json.dumps(state))
            logger.trace(f"[{self.node_id}] State saved: term={self.current_term}, voted_for={self.voted_for}, log_len={len(self.log)}")
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to save state: {e}")

    async def _load_state(self):
        """Load persistent state from Redis"""
        try:
            redis = await get_redis()
            data = await redis.get(f"raft:state:{self.node_id}")
            if data:
                state = json.loads(data)
                self.current_term = state["current_term"]
                self.voted_for = state["voted_for"]
                self.log = [LogEntry.from_dict(e) for e in state["log"]]
                logger.info(f"[{self.node_id}] State loaded: term={self.current_term}, log_len={len(self.log)}")
        except Exception as e:
            logger.warning(f"[{self.node_id}] Failed to load state (starting fresh): {e}")

    # =========================
    # HELPER FUNCTIONS
    # =========================
    def _random_election_timeout(self):
        """Generate random election timeout between 3-5 seconds"""
        return random.uniform(3.0, 5.0)
    
    def _last_log_index(self):
        """Get index of last log entry"""
        return len(self.log) - 1 if self.log else -1
    
    def _last_log_term(self):
        """Get term of last log entry"""
        return self.log[-1].term if self.log else 0
    
    def _get_majority_count(self):
        """Calculate majority count for cluster"""
        return (len(self.cluster_addrs) // 2) + 1

    # =========================
    # RPC HANDLERS (HTTP Routes)
    # =========================
    def routes(self):
        return {
            "/raft/request_vote": self.handle_request_vote,
            "/raft/append_entries": self.handle_append_entries,
            "/raft/leader_info": self.handle_leader_info,
            "/raft/commit_log": self.handle_commit_log  # New: client command endpoint
        }

    async def handle_request_vote(self, request):
        """
        RequestVote RPC Handler
        Invoked by candidates to gather votes
        """
        data = await request.json()
        term = data["term"]
        candidate_id = data["candidate_id"]
        last_log_index = data.get("last_log_index", -1)
        last_log_term = data.get("last_log_term", 0)

        # Rule 1: If term > currentTerm, update and convert to follower
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = "follower"
            await self._save_state()

        vote_granted = False
        
        # Rule 2: Grant vote if haven't voted and candidate's log is up-to-date
        if term >= self.current_term:
            if (self.voted_for is None or self.voted_for == candidate_id):
                # Check if candidate's log is at least as up-to-date as ours
                my_last_log_term = self._last_log_term()
                my_last_log_index = self._last_log_index()
                
                log_is_up_to_date = (
                    last_log_term > my_last_log_term or
                    (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
                )
                
                if log_is_up_to_date:
                    vote_granted = True
                    self.voted_for = candidate_id
                    self.heartbeat_received.set()  # Reset election timer
                    await self._save_state()

        logger.info(f"[{self.node_id}] Vote {'GRANTED' if vote_granted else 'DENIED'} to {candidate_id} (term={term})")
        return web.json_response({
            "term": self.current_term,
            "vote_granted": vote_granted
        })

    async def handle_append_entries(self, request):
        """
        AppendEntries RPC Handler
        Invoked by leader to replicate log entries and send heartbeats
        """
        data = await request.json()
        term = data["term"]
        leader_id = data["leader_id"]
        prev_log_index = data.get("prev_log_index", -1)
        prev_log_term = data.get("prev_log_term", 0)
        entries = data.get("entries", [])
        leader_commit = data.get("leader_commit", 0)

        success = False

        # Rule 1: Reply false if term < currentTerm
        if term < self.current_term:
            return web.json_response({
                "term": self.current_term,
                "success": False
            })

        # Rule 2: If term >= currentTerm, update and convert to follower
        if term >= self.current_term:
            self.current_term = term
            self.state = "follower"
            self.leader_id = leader_id
            self.heartbeat_received.set()
            await self._save_state()

        # Rule 3: Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term:
                logger.debug(f"[{self.node_id}] Log inconsistency at index {prev_log_index}")
                return web.json_response({
                    "term": self.current_term,
                    "success": False
                })

        # Rule 4: If existing entry conflicts with new one, delete it and all that follow
        if entries:
            for i, entry_dict in enumerate(entries):
                entry = LogEntry.from_dict(entry_dict)
                index = prev_log_index + 1 + i
                
                if index < len(self.log):
                    if self.log[index].term != entry.term:
                        # Delete conflicting entry and all that follow
                        self.log = self.log[:index]
                        logger.debug(f"[{self.node_id}] Truncated log at index {index}")
                
                # Append new entry
                if index >= len(self.log):
                    self.log.append(entry)
                    logger.debug(f"[{self.node_id}] Appended log entry at index {index}")
            
            await self._save_state()

        # Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self._last_log_index())
            logger.debug(f"[{self.node_id}] Updated commit_index to {self.commit_index}")
            # Apply committed entries to state machine
            await self._apply_committed_entries()

        success = True
        return web.json_response({
            "term": self.current_term,
            "success": success,
            "match_index": self._last_log_index()
        })

    async def handle_leader_info(self, request):
        """Return current leader information"""
        return web.json_response({
            "leader_id": self.leader_id,
            "term": self.current_term,
            "state": self.state,
            "commit_index": self.commit_index,
            "log_length": len(self.log)
        })

    async def handle_commit_log(self, request):
        """
        Client command endpoint
        Clients send commands here to be replicated
        """
        if self.state != "leader":
            if self.leader_id:
                leader_addr = next((addr for addr in self.cluster_addrs if self.leader_id in addr), None)
                if leader_addr:
                    return web.json_response({
                        "status": "redirect",
                        "leader": leader_addr
                    }, status=307)
            return web.json_response({
                "status": "error",
                "message": "No leader available"
            }, status=503)

        # Leader: append to log and replicate
        data = await request.json()
        command = data.get("command")
        
        entry = LogEntry(
            term=self.current_term,
            command=command,
            index=len(self.log)
        )
        self.log.append(entry)
        await self._save_state()
        
        logger.info(f"[{self.node_id}] Client command appended to log: {command}")
        
        # Trigger immediate replication
        asyncio.create_task(self._replicate_logs())
        
        return web.json_response({
            "status": "accepted",
            "index": entry.index,
            "term": entry.term
        })

    # =========================
    # STATE MACHINE APPLICATION
    # =========================
    async def _apply_committed_entries(self):
        """Apply committed log entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            logger.info(f"[{self.node_id}] Applying entry {self.last_applied}: {entry.command}")
            # Here you would apply the command to your state machine
            # For now, just log it
            # TODO: Integrate with lock_manager, queue_node, cache_node

    # =========================
    # MAIN RAFT LOOP
    # =========================
    async def start(self):
        """Start Raft node"""
        self.running = True
        await self._load_state()
        asyncio.create_task(self.run())
        logger.success(f"[{self.node_id}] Raft consensus started (term={self.current_term})")

    async def stop(self):
        """Stop Raft node"""
        self.running = False
        await self._save_state()
        logger.info(f"[{self.node_id}] Raft consensus stopped")

    async def run(self):
        """Main Raft state machine loop"""
        while self.running:
            if self.state == "follower":
                await self._run_follower()
            elif self.state == "candidate":
                await self._run_candidate()
            elif self.state == "leader":
                await self._run_leader()
            await asyncio.sleep(0.1)

    # =========================
    # FOLLOWER STATE
    # =========================
    async def _run_follower(self):
        """Follower waits for heartbeat or starts election on timeout"""
        self.heartbeat_received.clear()
        self.election_timeout = self._random_election_timeout()
        
        try:
            await asyncio.wait_for(
                self.heartbeat_received.wait(),
                timeout=self.election_timeout
            )
            # Heartbeat received, stay as follower
        except asyncio.TimeoutError:
            # No heartbeat, start election
            logger.warning(f"[{self.node_id}] Election timeout ({self.election_timeout:.2f}s), starting election")
            self.state = "candidate"

    # =========================
    # CANDIDATE STATE
    # =========================
    async def _run_candidate(self):
        """Candidate runs election to become leader"""
        self.current_term += 1
        self.voted_for = self.node_id
        self.election_count += 1
        await self._save_state()
        
        votes_received = 1  # Vote for self
        logger.info(f"[{self.node_id}] Starting election for term {self.current_term}")
        
        # Request votes from all other nodes
        tasks = []
        for addr in self.cluster_addrs:
            if self.node_id in addr:
                continue
            # Skip failed nodes (if failure detector is available)
            if hasattr(self, 'failure_detector') and addr in self.failure_detector.failed_nodes:
                logger.debug(f"[{self.node_id}] Skipping failed node {addr}")
                continue
            
            tasks.append(self._request_vote_from_node(addr))
        
        # Wait for all votes (with timeout)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count votes
        for result in results:
            if isinstance(result, dict) and result.get("vote_granted"):
                votes_received += 1
        
        majority = self._get_majority_count()
        logger.info(f"[{self.node_id}] Election result: {votes_received}/{len(self.cluster_addrs)} votes (need {majority})")
        
        # Check if won election
        if votes_received >= majority and self.state == "candidate":
            self._become_leader()
        else:
            # Lost election or discovered higher term, revert to follower
            self.state = "follower"
            await asyncio.sleep(self._random_election_timeout())

    async def _request_vote_from_node(self, addr):
        """Send RequestVote RPC to a node"""
        try:
            response = await send_json(addr, "/raft/request_vote", {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self._last_log_index(),
                "last_log_term": self._last_log_term()
            }, timeout=2.0)
            
            if response and response.get("term", 0) > self.current_term:
                # Discovered higher term, step down
                self.current_term = response["term"]
                self.state = "follower"
                self.voted_for = None
                await self._save_state()
            
            return response
        except Exception as e:
            logger.debug(f"[{self.node_id}] Failed to get vote from {addr}: {e}")
            return None

    def _become_leader(self):
        """Transition to leader state"""
        self.state = "leader"
        self.leader_id = self.node_id
        
        # Initialize leader state
        last_log_index = self._last_log_index()
        for addr in self.cluster_addrs:
            self.next_index[addr] = last_log_index + 1
            self.match_index[addr] = 0
        
        logger.success(f"[{self.node_id}] 🎉 BECAME LEADER for term {self.current_term}")

    # =========================
    # LEADER STATE
    # =========================
    async def _run_leader(self):
        """Leader sends periodic heartbeats and replicates logs"""
        # Send heartbeat/log replication
        await self._replicate_logs()
        
        # Wait for heartbeat interval
        await asyncio.sleep(self.heartbeat_interval)

    async def _replicate_logs(self):
        """Replicate logs to all followers"""
        self.heartbeat_count += 1
        
        tasks = []
        for addr in self.cluster_addrs:
            if self.node_id in addr:
                continue
            if hasattr(self, 'failure_detector') and addr in self.failure_detector.failed_nodes:
                continue
            
            tasks.append(self._send_append_entries_to_node(addr))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update commit index based on match_index
        await self._update_commit_index()

    async def _send_append_entries_to_node(self, addr):
        """Send AppendEntries RPC to a follower"""
        try:
            next_idx = self.next_index.get(addr, 0)
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.log) else 0
            
            # Get entries to send
            entries = []
            if next_idx < len(self.log):
                entries = [e.to_dict() for e in self.log[next_idx:]]
            
            response = await send_json(addr, "/raft/append_entries", {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self.commit_index
            }, timeout=2.0)
            
            if response:
                if response.get("term", 0) > self.current_term:
                    # Discovered higher term, step down
                    self.current_term = response["term"]
                    self.state = "follower"
                    self.leader_id = None
                    await self._save_state()
                    return
                
                if response.get("success"):
                    # Update next_index and match_index
                    if entries:
                        self.match_index[addr] = prev_log_index + len(entries)
                        self.next_index[addr] = self.match_index[addr] + 1
                        logger.debug(f"[{self.node_id}] Replicated {len(entries)} entries to {addr}")
                else:
                    # Log inconsistency, decrement next_index and retry
                    self.next_index[addr] = max(0, self.next_index[addr] - 1)
                    logger.debug(f"[{self.node_id}] Log inconsistency with {addr}, retrying...")
        
        except Exception as e:
            logger.debug(f"[{self.node_id}] Failed to replicate to {addr}: {e}")

    async def _update_commit_index(self):
        """Update commit index based on match_index from majority"""
        if self.state != "leader":
            return
        
        # Find highest N where majority of match_index[i] >= N
        for n in range(self.commit_index + 1, len(self.log)):
            if self.log[n].term != self.current_term:
                continue
            
            # Count how many nodes have replicated this entry
            replicated_count = 1  # Leader has it
            for addr in self.cluster_addrs:
                if self.node_id in addr:
                    continue
                if self.match_index.get(addr, 0) >= n:
                    replicated_count += 1
            
            if replicated_count >= self._get_majority_count():
                self.commit_index = n
                logger.info(f"[{self.node_id}] Updated commit_index to {n}")
                await self._apply_committed_entries()