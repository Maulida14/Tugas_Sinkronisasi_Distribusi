"""Custom Raft consensus implementation - Log-based consensus protocol"""

import asyncio
import random
import logging
import aiohttp
import hashlib
import re
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime

from ..core.types import (
    NodeState, LogEntry, Message, MessageType
)
from ..core.config import SystemConfig


logger = logging.getLogger(__name__)


class RaftNode:
    """Raft consensus node with log-based replication"""
    
    def __init__(self, node_id: str, peers: List[str], config: SystemConfig):
        self.node_id = node_id
        self.peers = peers
        self.config = config
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.commit_index = -1
        self.last_applied = -1
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None
        
        # Leader state
        self.next_index: Dict[str, int] = {peer: len(self.log) for peer in peers}
        self.match_index: Dict[str, int] = {peer: 0 for peer in peers}
        
        # Timers
        self._election_timer: Optional[asyncio.Task] = None
        self._heartbeat_timer: Optional[asyncio.Task] = None
        self._election_in_progress = False
        self._election_timeout = random.uniform(
            config.consensus.election_timeout_min,
            config.consensus.election_timeout_max
        )
        self._node_timeout_bias = self._compute_timeout_bias()
        
        # Callbacks
        self.on_state_changed: List[Callable[[NodeState], None]] = []
        self.on_commit: List[Callable[[LogEntry], None]] = []
        
        # Network
        self.peers_connected: Dict[str, bool] = {peer: False for peer in peers}


    async def start(self):
        """Start the Raft node"""
        logger.info(f"Node {self.node_id} starting Raft consensus")
        self._reset_election_timer()


    async def stop(self):
        """Stop the Raft node"""
        if self._election_timer:
            self._election_timer.cancel()
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()


    def _reset_election_timer(self):
        """Reset election timer"""
        if self._election_timer:
            self._election_timer.cancel()
        
        self._election_timeout = random.uniform(
            self.config.consensus.election_timeout_min,
            self.config.consensus.election_timeout_max
        ) + self._node_timeout_bias
        self._election_timer = asyncio.create_task(self._election_timeout_handler())


    def _compute_timeout_bias(self) -> float:
        """Add a stable per-node bias so elections don't collide continuously."""
        suffix_match = re.search(r"(\d+)$", self.node_id)
        if suffix_match:
            # Give each node a clearly separated election window.
            ordinal = max(0, int(suffix_match.group(1)) - 1)
            return 1.25 * ordinal

        digest = hashlib.md5(self.node_id.encode("utf-8")).digest()
        slot = digest[0] % 6
        return 0.75 * slot


    async def _election_timeout_handler(self):
        """Handle election timeout"""
        try:
            await asyncio.sleep(self._election_timeout)
            if self.state != NodeState.LEADER and not self._election_in_progress:
                await self._start_election()
        except asyncio.CancelledError:
            pass


    async def _start_election(self):
        """Start leader election"""
        if self._election_in_progress:
            return

        self._election_in_progress = True
        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        
        logger.info(f"Node {self.node_id} starting election for term {self.current_term}")
        
        # Notify state changed
        for callback in self.on_state_changed:
            callback(self.state)
        
        self._reset_election_timer()
        
        # Request votes from all peers
        votes_received = 1  # Vote for self
        tasks = []
        
        for peer in self.peers:
            tasks.append(self._request_vote_from_peer(peer))
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            votes_received += sum(1 for r in results if r is True)
            
            # Check if won election
            if self.state == NodeState.CANDIDATE and votes_received >= self._quorum_size():
                await self._become_leader()
            elif self.state == NodeState.CANDIDATE:
                # Still a candidate, wait for next timeout
                pass
        finally:
            self._election_in_progress = False


    async def _request_vote_from_peer(self, peer: str) -> bool:
        """Request vote from a peer"""
        logger.info(f"[{self.node_id}] Requesting vote from {peer} term={self.current_term}")
        last_log_term = self.log[-1].term if self.log else 0
        last_log_index = len(self.log) - 1
        
        message = Message(
            message_type=MessageType.REQUEST_VOTE,
            sender_id=self.node_id,
            term=self.current_term,
            data={
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
        )
        
        try:
            # Simulate network call
            response = await asyncio.wait_for(
                self._send_message_to_peer(peer, message),
                timeout=self.config.consensus.request_timeout
            )
            logger.info(f"[{self.node_id}] Vote response from {peer}: granted={response.data.get('vote_granted', False)}")
            if response.term > self.current_term:
                self.current_term = response.term
                self.voted_for = None
                await self._become_follower()
            return response.data.get("vote_granted", False)
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"[{self.node_id}] Vote timeout from {peer}: {e}")
            return False


    async def _become_leader(self):
        """Become leader"""
        self._election_in_progress = False
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        
        logger.info(f"Node {self.node_id} became leader for term {self.current_term}")
        
        # Notify state changed
        for callback in self.on_state_changed:
            callback(self.state)
        
        # Initialize leader state
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = 0
        
        # Start sending heartbeats
        await self._start_heartbeat()


    async def _become_follower(self, leader_id: Optional[str] = None):
        """Step down to follower and reset election state."""
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

        previous_state = self.state
        self.state = NodeState.FOLLOWER
        self.leader_id = leader_id
        self._election_in_progress = False
        self._reset_election_timer()

        if previous_state != NodeState.FOLLOWER:
            for callback in self.on_state_changed:
                callback(self.state)


    async def _start_heartbeat(self):
        """Start sending heartbeats"""
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
        
        self._heartbeat_timer = asyncio.create_task(self._heartbeat_loop())


    async def _heartbeat_loop(self):
        """Heartbeat loop for leader"""
        try:
            while self.state == NodeState.LEADER:
                tasks = []
                for peer in self.peers:
                    tasks.append(self._send_append_entries(peer))
                
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(self.config.consensus.heartbeat_interval)
        except asyncio.CancelledError:
            pass


    async def _send_append_entries(self, peer: str) -> bool:
        """Send append entries RPC to peer"""
        prev_log_index = self.next_index[peer] - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.log) else 0
        
        entries = []
        if self.next_index[peer] < len(self.log):
            entries = self.log[self.next_index[peer]:]
        
        message = Message(
            message_type=MessageType.APPEND_ENTRIES,
            sender_id=self.node_id,
            term=self.current_term,
            data={
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [
                    {
                        "term": e.term,
                        "index": e.index,
                        "command": e.command,
                        "data": e.data,
                    }
                    for e in entries
                ],
                "leader_commit": self.commit_index
            }
        )
        
        try:
            response = await asyncio.wait_for(
                self._send_message_to_peer(peer, message),
                timeout=self.config.consensus.request_timeout
            )
            
            if response.data.get("success", False):
                self.match_index[peer] = max(self.match_index[peer], prev_log_index + len(entries))
                self.next_index[peer] = self.match_index[peer] + 1
                self.peers_connected[peer] = True
                await self._update_commit_index()
                return True
            else:
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.voted_for = None
                    await self._become_follower()
                if self.next_index[peer] > 0:
                    self.next_index[peer] -= 1
                return False
        except (asyncio.TimeoutError, Exception) as e:
            self.peers_connected[peer] = False
            logger.debug(f"Failed to send append_entries to {peer}: {e}")
            return False


    async def _update_commit_index(self):
        """Update commit index based on replication"""
        if self.state != NodeState.LEADER:
            return
        
        match_indices = sorted([self.match_index[peer] for peer in self.peers] + [len(self.log) - 1])
        n = match_indices[len(match_indices) // 2]
        
        if n > self.commit_index and len(self.log) > n and self.log[n].term == self.current_term:
            self.commit_index = n
            await self._apply_committed_entries()


    def _quorum_size(self) -> int:
        """Return the number of votes/replicas required for quorum."""
        total_nodes = len(self.peers) + 1
        return (total_nodes // 2) + 1


    async def _apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            
            logger.debug(f"Node {self.node_id} applying entry: {entry}")
            for callback in self.on_commit:
                callback(entry)


    async def handle_message(self, message: Message) -> Message:
        """Handle incoming message"""
        if message.term > self.current_term:
            self.current_term = message.term
            self.voted_for = None
            await self._become_follower()
        
        if message.message_type == MessageType.REQUEST_VOTE:
            return await self._handle_request_vote(message)
        elif message.message_type == MessageType.APPEND_ENTRIES:
            return await self._handle_append_entries(message)

        return Message(
            message_type=message.message_type,
            sender_id=self.node_id,
            term=self.current_term,
            data={"error": f"Unsupported message type: {message.message_type}"}
        )


    async def _handle_request_vote(self, message: Message) -> Message:
        """Handle vote request"""
        grant = False
        
        if message.term < self.current_term:
            grant = False
        elif message.term == self.current_term and self.voted_for is not None and self.voted_for != message.sender_id:
            grant = False
        else:
            last_log_term = self.log[-1].term if self.log else 0
            last_log_index = len(self.log) - 1
            
            if (message.data.get("last_log_term", 0) > last_log_term or
                (message.data.get("last_log_term", 0) == last_log_term and
                 message.data.get("last_log_index", 0) >= last_log_index)):
                grant = True
                self.voted_for = message.sender_id
                self._reset_election_timer()
        
        return Message(
            message_type=MessageType.VOTE_RESPONSE,
            sender_id=self.node_id,
            term=self.current_term,
            data={"vote_granted": grant}
        )


    async def _handle_append_entries(self, message: Message) -> Message:
        """Handle append entries"""
        self._reset_election_timer()
        if self.state != NodeState.FOLLOWER:
            await self._become_follower(message.sender_id)
        self.leader_id = message.sender_id
        
        success = False
        
        prev_log_index = message.data.get("prev_log_index", 0)
        prev_log_term = message.data.get("prev_log_term", 0)
        
        if prev_log_index < len(self.log):
            if prev_log_index == -1 or self.log[prev_log_index].term == prev_log_term:
                success = True
                # Append new entries
                entries_data = message.data.get("entries", [])
                for i, entry_data in enumerate(entries_data):
                    idx = prev_log_index + 1 + i
                    if idx < len(self.log):
                        if self.log[idx].term != entry_data.get("term"):
                            self.log = self.log[:idx]
                    if idx >= len(self.log):
                        self.log.append(LogEntry(
                            term=entry_data.get("term"),
                            index=entry_data.get("index"),
                            command=entry_data.get("command"),
                            data=entry_data.get("data", {})
                        ))
                
                # Update commit index
                leader_commit = message.data.get("leader_commit", 0)
                if leader_commit > self.commit_index:
                    self.commit_index = min(leader_commit, len(self.log) - 1)
                    await self._apply_committed_entries()
        
        return Message(
            message_type=MessageType.APPEND_RESPONSE,
            sender_id=self.node_id,
            term=self.current_term,
            data={
                "success": success,
                "match_index": len(self.log) - 1 if success else 0
            }
        )


    async def append_entry(self, command: str, data: Dict[str, Any]) -> bool:
        """Append entry to log (for client requests)"""
        if self.state != NodeState.LEADER:
            logger.warning(f"Node {self.node_id} is not leader, cannot append entry")
            return False
        
        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command,
            data=data
        )
        
        self.log.append(entry)
        logger.debug(f"Node {self.node_id} appended entry: {entry}")
        
        # For single-node clusters, immediately commit
        if not self.peers:
            self.commit_index = entry.index
            await self._apply_committed_entries()
        else:
            # For multi-node clusters, replicate to followers
            await self._send_append_entries_to_all()
        
        return True


    async def _send_append_entries_to_all(self):
        """Send append entries to all peers"""
        tasks = []
        for peer in self.peers:
            tasks.append(self._send_append_entries(peer))
        await asyncio.gather(*tasks, return_exceptions=True)


    async def _send_message_to_peer(self, peer: str, message: Message) -> Message:
        """Send a Raft RPC to a peer over HTTP."""
        url = self._peer_url(peer)
        logger.debug(f"[{self.node_id}] Sending {message.message_type.value} to {url}")
        timeout = aiohttp.ClientTimeout(total=self.config.consensus.request_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=self.message_to_dict(message)) as response:
                response.raise_for_status()
                return self.message_from_dict(await response.json())


    def _peer_url(self, peer: str) -> str:
        """Normalize peer values such as node_2:8002 into a Raft RPC URL."""
        base_url = peer.rstrip("/") if peer.startswith(("http://", "https://")) else f"http://{peer}"
        return f"{base_url}/raft/message"


    @staticmethod
    def message_to_dict(message: Message) -> Dict[str, Any]:
        """Serialize a Raft message for HTTP transport."""
        return {
            "message_type": message.message_type.value,
            "sender_id": message.sender_id,
            "term": message.term,
            "data": message.data,
            "timestamp": message.timestamp.isoformat(),
        }


    @staticmethod
    def message_from_dict(data: Dict[str, Any]) -> Message:
        """Deserialize a Raft message from HTTP transport."""
        return Message(
            message_type=MessageType(data["message_type"]),
            sender_id=data["sender_id"],
            term=int(data["term"]),
            data=data.get("data", {}),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.utcnow(),
        )


    def is_leader(self) -> bool:
        """Check if node is leader"""
        return self.state == NodeState.LEADER


    def get_leader(self) -> Optional[str]:
        """Get current leader ID"""
        return self.leader_id


    def get_state(self) -> NodeState:
        """Get current node state"""
        return self.state
