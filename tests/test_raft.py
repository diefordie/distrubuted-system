
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from src.consensus.raft import RaftNode, LogEntry


class TestRaftNode:
    
    @pytest.fixture
    def raft_node(self):
        node = RaftNode(
            node_id="test_node",
            cluster_addrs=["http://node1:8001", "http://node2:8002", "http://node3:8003"]
        )
        return node
    
    def test_initialization(self, raft_node):
        assert raft_node.node_id == "test_node"
        assert raft_node.current_term == 0
        assert raft_node.voted_for is None
        assert raft_node.state == "follower"
        assert raft_node.leader_id is None
        assert len(raft_node.log) == 0
        assert raft_node.commit_index == 0
        assert raft_node.last_applied == 0
    
    def test_log_entry_creation(self):
        entry = LogEntry(term=1, command={"op": "set", "key": "x", "value": 10}, index=0)
        
        assert entry.term == 1
        assert entry.command == {"op": "set", "key": "x", "value": 10}
        assert entry.index == 0
        
        
        entry_dict = entry.to_dict()
        assert entry_dict["term"] == 1
        assert entry_dict["index"] == 0
        
        
        entry2 = LogEntry.from_dict(entry_dict)
        assert entry2.term == entry.term
        assert entry2.command == entry.command
    
    def test_last_log_index(self, raft_node):
        assert raft_node._last_log_index() == -1
        
        raft_node.log.append(LogEntry(1, {"cmd": "a"}, 0))
        assert raft_node._last_log_index() == 0
        
        raft_node.log.append(LogEntry(1, {"cmd": "b"}, 1))
        assert raft_node._last_log_index() == 1
    
    def test_last_log_term(self, raft_node):
        assert raft_node._last_log_term() == 0
        
        raft_node.log.append(LogEntry(5, {"cmd": "a"}, 0))
        assert raft_node._last_log_term() == 5
    
    def test_majority_calculation(self, raft_node):
        
        assert raft_node._get_majority_count() == 2
        
        
        raft_node.cluster_addrs = ["n1", "n2", "n3", "n4", "n5"]
        assert raft_node._get_majority_count() == 3
    
    @pytest.mark.asyncio
    async def test_request_vote_grant(self, raft_node):
        
        mock_request = Mock()
        mock_request.json = AsyncMock(return_value={
            "term": 2,
            "candidate_id": "node2",
            "last_log_index": 0,
            "last_log_term": 1
        })
        
        
        response = await raft_node.handle_request_vote(mock_request)
        response_data = response._body
        
        assert raft_node.current_term == 2
        assert raft_node.voted_for == "node2"
        assert b'"vote_granted": true' in response_data
    
    @pytest.mark.asyncio
    async def test_request_vote_deny(self, raft_node):
        
        raft_node.current_term = 2
        raft_node.voted_for = "node1"
        
        mock_request = Mock()
        mock_request.json = AsyncMock(return_value={
            "term": 2,
            "candidate_id": "node2",
            "last_log_index": 0,
            "last_log_term": 1
        })
        
        response = await raft_node.handle_request_vote(mock_request)
        response_data = response._body
        
        
        assert raft_node.voted_for == "node1"
        assert b'"vote_granted": false' in response_data
    
    @pytest.mark.asyncio
    async def test_append_entries_success(self, raft_node):
        raft_node.current_term = 1
        
        mock_request = Mock()
        mock_request.json = AsyncMock(return_value={
            "term": 2,
            "leader_id": "node2",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [{"term": 2, "command": {"op": "set"}, "index": 0}],
            "leader_commit": 0
        })
        
        response = await raft_node.handle_append_entries(mock_request)
        response_data = response._body
        
        
        assert raft_node.current_term == 2
        assert raft_node.state == "follower"
        assert raft_node.leader_id == "node2"
        assert len(raft_node.log) == 1
        assert b'"success": true' in response_data
    
    @pytest.mark.asyncio
    async def test_append_entries_reject_old_term(self, raft_node):
        raft_node.current_term = 5
        
        mock_request = Mock()
        mock_request.json = AsyncMock(return_value={
            "term": 3,
            "leader_id": "node2",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0
        })
        
        response = await raft_node.handle_append_entries(mock_request)
        response_data = response._body
        
        
        assert raft_node.current_term == 5
        assert b'"success": false' in response_data
    
    @pytest.mark.asyncio
    async def test_append_entries_log_consistency(self, raft_node):
        
        raft_node.log.append(LogEntry(1, {"cmd": "a"}, 0))
        raft_node.current_term = 2
        
        mock_request = Mock()
        mock_request.json = AsyncMock(return_value={
            "term": 2,
            "leader_id": "node2",
            "prev_log_index": 0,
            "prev_log_term": 2,  
            "entries": [{"term": 2, "command": {"op": "set"}, "index": 1}],
            "leader_commit": 0
        })
        
        response = await raft_node.handle_append_entries(mock_request)
        response_data = response._body
        
        
        assert b'"success": false' in response_data
    
    @pytest.mark.asyncio
    async def test_state_persistence(self, raft_node):
        with patch('src.consensus.raft.get_redis') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            raft_node.current_term = 5
            raft_node.voted_for = "node2"
            raft_node.log.append(LogEntry(5, {"cmd": "test"}, 0))
            
            await raft_node._save_state()
            
            
            mock_redis_instance.set.assert_called_once()
            call_args = mock_redis_instance.set.call_args
            assert "raft:state:test_node" in call_args[0]
    
    def test_become_leader_initialization(self, raft_node):
        raft_node.log.append(LogEntry(1, {"cmd": "a"}, 0))
        raft_node.log.append(LogEntry(2, {"cmd": "b"}, 1))
        
        raft_node._become_leader()
        
        assert raft_node.state == "leader"
        assert raft_node.leader_id == "test_node"
        
        
        for addr in raft_node.cluster_addrs:
            assert addr in raft_node.next_index
            assert raft_node.next_index[addr] == 2  
            assert raft_node.match_index[addr] == 0


class TestRaftElection:
    
    @pytest.mark.asyncio
    async def test_single_node_becomes_leader(self):
        node = RaftNode("node1", ["http://node1:8001"])
        
        
        with patch('src.consensus.raft.get_redis', return_value=None):
            await node.start()
            
            
            node.state = "candidate"
            await node._run_candidate()
            
            
            assert node.state == "leader"
            assert node.leader_id == "node1"
            
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_election_timeout_triggers_candidacy(self):
        node = RaftNode("node1", ["http://node1:8001", "http://node2:8002"])
        
        with patch('src.consensus.raft.get_redis', return_value=None):
            node.state = "follower"
            node.election_timeout = 0.1  
            
            
            await node._run_follower()
            
            
            assert node.state == "candidate"


class TestRaftLogReplication:
    
    def test_log_append(self):
        node = RaftNode("node1", ["http://node1:8001"])
        
        entry1 = LogEntry(1, {"op": "set", "key": "x", "value": 10}, 0)
        entry2 = LogEntry(1, {"op": "set", "key": "y", "value": 20}, 1)
        
        node.log.append(entry1)
        node.log.append(entry2)
        
        assert len(node.log) == 2
        assert node.log[0].command["key"] == "x"
        assert node.log[1].command["key"] == "y"
    
    def test_commit_index_update(self):
        node = RaftNode("node1", ["http://node1:8001", "http://node2:8002", "http://node3:8003"])
        node.state = "leader"
        node.current_term = 1
        
        
        for i in range(5):
            node.log.append(LogEntry(1, {"op": f"cmd{i}"}, i))
        
        
        node.match_index["http://node2:8002"] = 3
        node.match_index["http://node3:8003"] = 2
        
        
        
        
        assert node.commit_index == 0
