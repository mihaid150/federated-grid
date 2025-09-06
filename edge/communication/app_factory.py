from edge.communication.coordinator import EdgeCoordinator
from shared.logging_config import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from shared.node_state import FederatedNodeState
from shared.shared_main import shared_router
from edge.communication.edge_service import EdgeService
from shared.monitoring_thread import MonitoringThread

app = FastAPI()

coord = EdgeCoordinator()
edge_service = EdgeService(coord)
coord.attach_service(edge_service)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(shared_router, prefix="/node")


@app.on_event("startup")
async def startup_event():
    logger.info("Initializing edge application...")
    monitoring_thread = None

    def start_listeners_when_node_is_ready(already_started=None):
        if already_started is None:
            already_started = {'done': False}
        node = FederatedNodeState.get_current_node()
        if not already_started['done'] and node is not None:
            logger.info("FederatedNodeState initialized! Starting AMQP/MQTT listeners...")
            coord.start_background_consumers()
            already_started["done"] = True
            monitoring_thread.stop()

    monitoring_thread = MonitoringThread(start_listeners_when_node_is_ready, 10)
    monitoring_thread.start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)