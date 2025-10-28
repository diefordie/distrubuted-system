
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
    
    def __init__(self, node_id=NODE_ID, cluster_addrs=CLUSTER_ADDRS):
        self.node_id = node_id
        self.cluster_addrs = cluster_addrs
        
        
        self.current_term = 0
        self.voted_for = None
        self.log = []  
        
        
        self.commit_index = 0
        self.last_applied = 0
        
        
        self.next_index = {}   
        self.match_index = {}  
        
        
        self.state = "follower"  
        self.leader_id = None
        
        
        self.heartbeat_received = asyncio.Event()
        self.running = False
        self.election_timeout = self._random_election_timeout()
        self.heartbeat_interval = 1.0  
        
        
        self.election_count = 0
        self.heartbeat_count = 0

    
    
    
    async def _save_state(self):
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

    
    
    
    def _random_election_timeout(self):
        return random.uniform(3.0, 5.0)
    
    def _last_log_index(self):
        return len(self.log) - 1 if self.log else -1
    
    def _last_log_term(self):
        return self.log[-1].term if self.log else 0
    
    def _get_majority_count(self):
        return (len(self.cluster_addrs) // 2) + 1

    
    
    
    def routes(self):
        return {
            "/raft/request_vote": self.handle_request_vote,
            "/raft/append_entries": self.handle_append_entries,
            "/raft/leader_info": self.handle_leader_info,
            "/raft/commit_log": self.handle_commit_log  
        }

    async def handle_request_vote(self, request):
        data = await request.json()
        term = data["term"]
        candidate_id = data["candidate_id"]
        last_log_index = data.get("last_log_index", -1)
        last_log_term = data.get("last_log_term", 0)

        
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = "follower"
            await self._save_state()

        vote_granted = False
        
        
        if term >= self.current_term:
            if (self.voted_for is None or self.voted_for == candidate_id):
                
                my_last_log_term = self._last_log_term()
                my_last_log_index = self._last_log_index()
                
                log_is_up_to_date = (
                    last_log_term > my_last_log_term or
                    (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
                )
                
                if log_is_up_to_date:
                    vote_granted = True
                    self.voted_for = candidate_id
                    self.heartbeat_received.set()  
                    await self._save_state()

        logger.info(f"[{self.node_id}] Vote {'GRANTED' if vote_granted else 'DENIED'} to {candidate_id} (term={term})")
        return web.json_response({
            "term": self.current_term,
            "vote_granted": vote_granted
        })

    async def handle_append_entries(self, request):
        data = await request.json()
        term = data["term"]
        leader_id = data["leader_id"]
        prev_log_index = data.get("prev_log_index", -1)
        prev_log_term = data.get("prev_log_term", 0)
        entries = data.get("entries", [])
        leader_commit = data.get("leader_commit", 0)

        success = False

        
        if term < self.current_term:
            return web.json_response({
                "term": self.current_term,
                "success": False
            })

        
        if term >= self.current_term:
            self.current_term = term
            self.state = "follower"
            self.leader_id = leader_id
            self.heartbeat_received.set()
            await self._save_state()

        
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term:
                logger.debug(f"[{self.node_id}] Log inconsistency at index {prev_log_index}")
                return web.json_response({
                    "term": self.current_term,
                    "success": False
                })

        
        if entries:
            for i, entry_dict in enumerate(entries):
                entry = LogEntry.from_dict(entry_dict)
                index = prev_log_index + 1 + i
                
                if index < len(self.log):
                    if self.log[index].term != entry.term:
                        
                        self.log = self.log[:index]
                        logger.debug(f"[{self.node_id}] Truncated log at index {index}")
                
                
                if index >= len(self.log):
                    self.log.append(entry)
                    logger.debug(f"[{self.node_id}] Appended log entry at index {index}")
            
            await self._save_state()

        
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self._last_log_index())
            logger.debug(f"[{self.node_id}] Updated commit_index to {self.commit_index}")
            
            await self._apply_committed_entries()

        success = True
        return web.json_response({
            "term": self.current_term,
            "success": success,
            "match_index": self._last_log_index()
        })

    async def handle_leader_info(self, request):
        return web.json_response({
            "leader_id": self.leader_id,
            "term": self.current_term,
            "state": self.state,
            "commit_index": self.commit_index,
            "log_length": len(self.log)
        })

    async def handle_commit_log(self, request):
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
        
        
        asyncio.create_task(self._replicate_logs())
        
        return web.json_response({
            "status": "accepted",
            "index": entry.index,
            "term": entry.term
        })

    
    
    
    async def _apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            logger.info(f"[{self.node_id}] Applying entry {self.last_applied}: {entry.command}")
            
            
            

    
    
    
    async def start(self):
        self.running = True
        await self._load_state()
        asyncio.create_task(self.run())
        logger.success(f"[{self.node_id}] Raft consensus started (term={self.current_term})")

    async def stop(self):
        self.running = False
        await self._save_state()
        logger.info(f"[{self.node_id}] Raft consensus stopped")

    async def run(self):
        while self.running:
            if self.state == "follower":
                await self._run_follower()
            elif self.state == "candidate":
                await self._run_candidate()
            elif self.state == "leader":
                await self._run_leader()
            await asyncio.sleep(0.1)

    
    
    
    async def _run_follower(self):
        self.heartbeat_received.clear()
        self.election_timeout = self._random_election_timeout()
        
        try:
            await asyncio.wait_for(
                self.heartbeat_received.wait(),
                timeout=self.election_timeout
            )
            
        except asyncio.TimeoutError:
            
            logger.warning(f"[{self.node_id}] Election timeout ({self.election_timeout:.2f}s), starting election")
            self.state = "candidate"

    
    
    
    async def _run_candidate(self):
        self.current_term += 1
        self.voted_for = self.node_id
        self.election_count += 1
        await self._save_state()
        
        votes_received = 1  
        logger.info(f"[{self.node_id}] Starting election for term {self.current_term}")
        
        
        tasks = []
        for addr in self.cluster_addrs:
            if self.node_id in addr:
                continue
            
            if hasattr(self, 'failure_detector') and addr in self.failure_detector.failed_nodes:
                logger.debug(f"[{self.node_id}] Skipping failed node {addr}")
                continue
            
            tasks.append(self._request_vote_from_node(addr))
        
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        
        for result in results:
            if isinstance(result, dict) and result.get("vote_granted"):
                votes_received += 1
        
        majority = self._get_majority_count()
        logger.info(f"[{self.node_id}] Election result: {votes_received}/{len(self.cluster_addrs)} votes (need {majority})")
        
        
        if votes_received >= majority and self.state == "candidate":
            self._become_leader()
        else:
            
            self.state = "follower"
            await asyncio.sleep(self._random_election_timeout())

    async def _request_vote_from_node(self, addr):
        try:
            response = await send_json(addr, "/raft/request_vote", {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self._last_log_index(),
                "last_log_term": self._last_log_term()
            }, timeout=2.0)
            
            if response and response.get("term", 0) > self.current_term:
                
                self.current_term = response["term"]
                self.state = "follower"
                self.voted_for = None
                await self._save_state()
            
            return response
        except Exception as e:
            logger.debug(f"[{self.node_id}] Failed to get vote from {addr}: {e}")
            return None

    def _become_leader(self):
        self.state = "leader"
        self.leader_id = self.node_id
        
        
        last_log_index = self._last_log_index()
        for addr in self.cluster_addrs:
            self.next_index[addr] = last_log_index + 1
            self.match_index[addr] = 0
        
        logger.success(f"[{self.node_id}] 🎉 BECAME LEADER for term {self.current_term}")

    
    
    
    async def _run_leader(self):
        
        await self._replicate_logs()
        
        
        await asyncio.sleep(self.heartbeat_interval)

    async def _replicate_logs(self):
        self.heartbeat_count += 1
        
        tasks = []
        for addr in self.cluster_addrs:
            if self.node_id in addr:
                continue
            if hasattr(self, 'failure_detector') and addr in self.failure_detector.failed_nodes:
                continue
            
            tasks.append(self._send_append_entries_to_node(addr))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        
        await self._update_commit_index()

    async def _send_append_entries_to_node(self, addr):
        try:
            next_idx = self.next_index.get(addr, 0)
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.log) else 0
            
            
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
                    
                    self.current_term = response["term"]
                    self.state = "follower"
                    self.leader_id = None
                    await self._save_state()
                    return
                
                if response.get("success"):
                    
                    if entries:
                        self.match_index[addr] = prev_log_index + len(entries)
                        self.next_index[addr] = self.match_index[addr] + 1
                        logger.debug(f"[{self.node_id}] Replicated {len(entries)} entries to {addr}")
                else:
                    
                    self.next_index[addr] = max(0, self.next_index[addr] - 1)
                    logger.debug(f"[{self.node_id}] Log inconsistency with {addr}, retrying...")
        
        except Exception as e:
            logger.debug(f"[{self.node_id}] Failed to replicate to {addr}: {e}")

    async def _update_commit_index(self):
        if self.state != "leader":
            return
        
        
        for n in range(self.commit_index + 1, len(self.log)):
            if self.log[n].term != self.current_term:
                continue
            
            
            replicated_count = 1  
            for addr in self.cluster_addrs:
                if self.node_id in addr:
                    continue
                if self.match_index.get(addr, 0) >= n:
                    replicated_count += 1
            
            if replicated_count >= self._get_majority_count():
                self.commit_index = n
                logger.info(f"[{self.node_id}] Updated commit_index to {n}")
                await self._apply_committed_entries()
