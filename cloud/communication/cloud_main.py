from typing import Dict, Any
import threading
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from shared.commands import Command
from shared.logging_config import logger
from cloud.communication.cloud_messaging import CloudMessaging

cloud_router = APIRouter()


class NotifyModelCreation(Command):
    def __init__(self, cloud_main):
        self.cloud_main = cloud_main

    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return self.cloud_main.notify_model_creation()


class NotifyFirstTraining(Command):
    def __init__(self, cloud_main):
        self.cloud_main = cloud_main

    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return self.cloud_main.notify_for_first_training(data)


class BroadcastCloudModel(Command):
    def __init__(self, cloud_main):
        self.cloud_main = cloud_main

    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return self.cloud_main.broadcast_cloud_model(data)


class CloudMain:
    def __init__(self):
        self.cloud_messaging = CloudMessaging()

        self.command_map = {
            0: NotifyModelCreation(self),
            1: NotifyFirstTraining(self),
            2: BroadcastCloudModel(self),
        }

        for cmd in self.command_map.values():
            cmd.cloud_main = self

    def notify_model_creation(self) -> Dict[str, any]:
        self.cloud_messaging.notify_all_edges_to_create_local_model()
        return {"message": "Cloud (MQTT): sent command to fogs instructing edges to create local model."}

    def notify_for_first_training(self, data: Dict[str, any]) -> Dict[str, any]:
        self.cloud_messaging.notify_all_edges_to_start_first_training(data)
        return {"message": "Cloud (MQTT): sent command to fogs instructing edges to start the first training."}

    def broadcast_cloud_model(self, data: Dict[str, any]) -> Dict[str, any]:
        self.cloud_messaging.broadcast_cloud_model(data)
        return {"message": "Cloud (AMQP): broadcast cloud model to fogs."}

    async def websocket_handler(self, websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                message: Dict[str, Any] = await websocket.receive_json()
                operation: int = message.get('operation')
                data: Dict[str, Any] = message.get('data', {})

                try:
                    command = self.command_map.get(operation)
                    if command:
                        response = command.execute(data)
                    else:
                        response = {"Error": f"Invalid Operation {operation}."}
                except Exception as e:
                    response = {"Error": str(e)}

                await websocket.send_json(response)
        except WebSocketDisconnect:
            logger.warning("WebSocket disconnected...")
