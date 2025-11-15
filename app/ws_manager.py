import logging
from typing import List, Dict
from fastapi import WebSocket

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Manages active WebSocket connections.
    """
    def __init__(self):
        # A dictionary to hold active connections, mapping client_id to WebSocket object
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        """
        Accepts a new WebSocket connection and adds it to the active connections.
        """
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(f"New connection accepted for client_id: {client_id}. Total connections: {len(self.active_connections)}")

    def disconnect(self, client_id: str):
        """
        Removes a WebSocket connection from the active connections.
        """
        if client_id in self.active_connections:
            del self.active_connections[client_id]
            logger.info(f"Connection closed for client_id: {client_id}. Total connections: {len(self.active_connections)}")

    async def send_personal_message(self, message: str, client_id: str):
        """
        Sends a message to a specific client.
        """
        if client_id in self.active_connections:
            websocket = self.active_connections[client_id]
            await websocket.send_text(message)
            logger.info(f"Sent message to {client_id}: {message}")
        else:
            logger.warning(f"Attempted to send message to disconnected client_id: {client_id}")

    async def broadcast(self, message: str):
        """
        Broadcasts a message to all connected clients.
        """
        logger.info(f"Broadcasting message to {len(self.active_connections)} clients: {message}")
        for client_id, websocket in self.active_connections.items():
            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.error(f"Failed to send message to client {client_id}: {e}")

# Create a singleton instance of the ConnectionManager
manager = ConnectionManager()