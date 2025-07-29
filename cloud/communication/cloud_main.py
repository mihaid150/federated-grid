from typing import Dict, Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from shared.commands import Command
from shared.logging_config import logger

cloud_router = APIRouter()


class NotifyModelCreation(Command):
    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return notify_model_creation()


command_map = {
    0: NotifyModelCreation()
}


@cloud_router.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            message: Dict[str, Any] = await websocket.receive_json()
            operation: int = message.get('operation')
            data: Dict[str, Any] = message.get('data', {})

            try:
                command = command_map.get(operation)
                if command:
                    response = command.execute(data)
                else:
                    response = {"Error": f"Invalid Operation {command}."}
            except Exception as e:
                response = {"Error": str(e)}

            await websocket.send_json(response)
    except WebSocketDisconnect:
        logger.warning("WebSocket disconnected...")


def notify_model_creation() -> Dict[str, any]:
    pass