# cloud/communication/cloud_main.py
from typing import Dict, Any
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from shared.commands_base import Command
from shared.logging_config import logger
from cloud.communication.coordinator import CloudCoordinator
from cloud.communication.scheduler import CloudRoundScheduler

cloud_router = APIRouter()


class NotifyModelCreation(Command):
    def __init__(self, cloud_main):
        self.cloud_main = cloud_main

    def execute(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.cloud_main.notify_model_creation()


class NotifyFirstTraining(Command):
    def __init__(self, cloud_main):
        self.cloud_main = cloud_main

    def execute(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.cloud_main.notify_for_first_training(data)


class DispatchCloudModel(Command):
    def __init__(self, cloud_main):
        self.cloud_main = cloud_main

    def execute(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.cloud_main.dispatch_cloud_model(data)


class RunSchedule(Command):
    def __init__(self, cloud_main):
        self.cloud_main = cloud_main

    def execute(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.cloud_main.start_schedule(data)


class CloudMain:
    def __init__(self):
        # use the new façade
        self.coordinator = CloudCoordinator()
        # scheduler for automated rounds
        self._scheduler = CloudRoundScheduler(self.coordinator.cfg, self.coordinator.commands, self.coordinator.state)

        self.command_map = {
            0: NotifyModelCreation(self),
            1: NotifyFirstTraining(self),
            2: DispatchCloudModel(self),
            3: RunSchedule(self),
        }
        for cmd in self.command_map.values():
            cmd.cloud_main = self

    # ----- API invoked by commands -----

    def notify_model_creation(self) -> Dict[str, Any]:
        self.coordinator.notify_all_edges_to_create_local_model()
        return {"message": "Cloud (MQTT): sent command to fogs instructing edges to create local model."}

    def notify_for_first_training(self, data: Dict[str, Any]) -> Dict[str, Any]:
        self.coordinator.notify_all_edges_to_start_first_training(data)
        return {"message": "Cloud (MQTT): sent command to fogs instructing edges to start the first training."}

    def dispatch_cloud_model(self, data: Dict[str, Any]) -> Dict[str, Any]:
        self.coordinator.dispatch_cloud_model(data)
        return {"message": "Cloud (AMQP): dispatch cloud model to fogs."}

    def start_schedule(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Start a background period scheduler.
        Expected keys in data:
          - start-period: YYYY-MM-DD
          - end-period: YYYY-MM-DD
          - metric: e.g. 'r2'
          - threshold: float
          - rounds-reached-threshold: int
          - maximum-cycles: int
          - cooldown_seconds: int (optional, default 5)
        """
        try:
            sim_info = self._scheduler.start(data)
            response = {"message": "Cloud: schedule started", "params": data}
            if sim_info:
                response["simulation"] = sim_info
            return response
        except Exception as e:
            logger.exception("Cloud: failed to start schedule: %s", e)
            return {"Error": str(e)}

    # ----- WS handler remains the same -----
    async def websocket_handler(self, websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                message: Dict[str, Any] = await websocket.receive_json()
                operation: int = message.get("operation")
                data: Dict[str, Any] = message.get("data", {}) or {}

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
