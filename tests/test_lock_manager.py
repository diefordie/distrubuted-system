
import pytest
import pytest_asyncio
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch
from src.nodes.lock_manager import LockManager, Lock


class TestLock:
    
    def test_lock_creation(self):
        lock = Lock("resource1", "exclusive", {"client1"}, 1000.0, 300)
        
        assert lock.resource == "resource1"
        assert lock.mode == "exclusive"
        assert "client1" in lock.owners
        assert lock.ttl == 300
    
    def test_lock_serialization(self):
        lock = Lock("resource1", "shared", {"client1", "client2"}, 1000.0)
        
        lock_dict = lock.to_dict()
        assert lock_dict["resource"] == "resource1"
        assert lock_dict["mode"] == "shared"
        assert set(lock_dict["owners"]) == {"client1", "client2"}
        
        
        lock2 = Lock.from_dict(lock_dict)
        assert lock2.resource == lock.resource
        assert lock2.mode == lock.mode
        assert lock2.owners == lock.owners
    
    def test_lock_expiration(self):
        
        lock = Lock("resource1", "exclusive", {"client1"}, time.time(), ttl=1)
        
        
        assert not lock.is_expired()
        
        
        time.sleep(1.1)
        assert lock.is_expired()


class TestLockManager:
    
    @pytest.fixture
    def mock_node(self):
        node = Mock()
        node.node_id = "test_node"
        
        
        node.raft = Mock()
        node.raft.state = "leader"
        node.raft.leader_id = "test_node"
        node.raft.cluster_addrs = ["http://node1:8001", "http://node2:8002"]
        
        
        node.metrics = Mock()
        node.metrics.record_lock_acquire = Mock()
        node.metrics.record_lock_release = Mock()
        node.metrics.record_deadlock = Mock()
        node.metrics.update_active_locks = Mock()
        
        return node
    
    @pytest_asyncio.fixture  
    async def lock_manager(self, mock_node):
        manager = LockManager(mock_node)
        
        yield manager
        
        if manager._cleanup_task:
            await manager.stop()
    
    @pytest.mark.asyncio
    async def test_exclusive_lock_grant(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            await lock_manager._grant_lock("resource1", "exclusive", "client1")
        
        assert "resource1" in lock_manager.locks
        assert "client1" in lock_manager.locks["resource1"].owners
        assert lock_manager.locks["resource1"].mode == "exclusive"
        mock_node.metrics.record_lock_acquire.assert_called_with(success=True)
    
    @pytest.mark.asyncio
    async def test_shared_lock_compatibility(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            await lock_manager._grant_lock("resource1", "shared", "client1")
            
            
            can_grant, reason = await lock_manager._can_grant_lock("resource1", "shared", "client2")
            assert can_grant
            assert reason == "shared_compatible"
            
            await lock_manager._grant_lock("resource1", "shared", "client2")
            
            
            assert "client1" in lock_manager.locks["resource1"].owners
            assert "client2" in lock_manager.locks["resource1"].owners
    
    @pytest.mark.asyncio
    async def test_exclusive_lock_blocking(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            await lock_manager._grant_lock("resource1", "exclusive", "client1")
            
            
            can_grant, reason = await lock_manager._can_grant_lock("resource1", "exclusive", "client2")
            assert not can_grant
            
            can_grant, reason = await lock_manager._can_grant_lock("resource1", "shared", "client2")
            assert not can_grant
    
    @pytest.mark.asyncio
    async def test_lock_release(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            await lock_manager._grant_lock("resource1", "exclusive", "client1")
            assert "resource1" in lock_manager.locks
            
            
            await lock_manager._release_lock_internal("resource1", "client1")
            assert "resource1" not in lock_manager.locks
    
    @pytest.mark.asyncio
    async def test_shared_lock_partial_release(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            await lock_manager._grant_lock("resource1", "shared", "client1")
            await lock_manager._grant_lock("resource1", "shared", "client2")
            
            
            await lock_manager._release_lock_internal("resource1", "client1")
            
            
            assert "resource1" in lock_manager.locks
            assert "client1" not in lock_manager.locks["resource1"].owners
            assert "client2" in lock_manager.locks["resource1"].owners
            
            
            await lock_manager._release_lock_internal("resource1", "client2")
            
            
            assert "resource1" not in lock_manager.locks
    
    @pytest.mark.asyncio
    async def test_deadlock_detection_simple(self, lock_manager, mock_node):
        
        
        
        
        lock_manager.locks["resource1"] = Lock("resource1", "exclusive", {"client1"}, 1000.0)
        lock_manager.locks["resource2"] = Lock("resource2", "exclusive", {"client2"}, 1000.0)
        
        lock_manager.wait_for_graph["client1"] = {"resource2"}
        lock_manager.wait_for_graph["client2"] = {"resource1"}
        
        
        has_deadlock = lock_manager._detect_deadlock("client2", "resource1")
        
        
        assert has_deadlock
        mock_node.metrics.record_deadlock.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_acquire_handler_success(self, lock_manager, mock_node):
        mock_request = Mock()
        mock_request.json = AsyncMock(return_value={
            "resource": "db1",
            "client_id": "client1",
            "mode": "exclusive"
        })
        
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            response = await lock_manager.acquire_handler(mock_request)
            response_data = response._body
            
            assert b'"status": "granted"' in response_data
            assert "db1" in lock_manager.locks

    @pytest.mark.asyncio
    async def test_acquire_handler_denied(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            await lock_manager._grant_lock("db1", "exclusive", "client1")
            
            
            mock_request = Mock()
            mock_request.json = AsyncMock(return_value={
                "resource": "db1",
                "client_id": "client2",
                "mode": "exclusive",
                "wait": False
            })
            
            response = await lock_manager.acquire_handler(mock_request)
            response_data = response._body
            
            assert b'"status": "denied"' in response_data
    
    @pytest.mark.asyncio
    async def test_acquire_handler_deadlock(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            
            current_time = time.time()
            lock_manager.locks["resource1"] = Lock("resource1", "exclusive", {"client1"}, current_time)
            lock_manager.locks["resource2"] = Lock("resource2", "exclusive", {"client2"}, current_time)
            lock_manager.wait_for_graph["client1"] = {"resource2"}
            lock_manager.wait_for_graph["client2"] = {"resource1"}
            
            mock_request = Mock()
            mock_request.json = AsyncMock(return_value={
                "resource": "resource1",
                "client_id": "client2",
                "mode": "exclusive",
                "wait": False
            })
            
            response = await lock_manager.acquire_handler(mock_request)
            assert response.status == 409  
            response_data = response._body
            assert b'"status": "deadlock_detected"' in response_data
    
    @pytest.mark.asyncio
    async def test_release_handler_success(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            await lock_manager._grant_lock("db1", "exclusive", "client1")
            
            
            mock_request = Mock()
            mock_request.json = AsyncMock(return_value={
                "resource": "db1",
                "client_id": "client1"
            })
            
            response = await lock_manager.release_handler(mock_request)
            response_data = response._body
            
            assert b'"status": "released"' in response_data
            assert "db1" not in lock_manager.locks
    
    @pytest.mark.asyncio
    async def test_release_handler_not_found(self, lock_manager, mock_node):
        mock_request = Mock()
        mock_request.json = AsyncMock(return_value={
            "resource": "nonexistent",
            "client_id": "client1"
        })
        
        response = await lock_manager.release_handler(mock_request)
        assert response.status == 404
        response_data = response._body
        assert b'"status": "not_found"' in response_data
    
    @pytest.mark.asyncio
    async def test_release_handler_not_owner(self, lock_manager, mock_node):
        with patch('src.nodes.lock_manager.get_redis', return_value=None):
            
            await lock_manager._grant_lock("db1", "exclusive", "client1")
            
            
            mock_request = Mock()
            mock_request.json = AsyncMock(return_value={
                "resource": "db1",
                "client_id": "client2"
            })
            
            response = await lock_manager.release_handler(mock_request)
            assert response.status == 403
            response_data = response._body
            assert b'"status": "not_owner"' in response_data
