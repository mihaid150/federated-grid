import threading

from shared.logging_config import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from prometheus_fastapi_instrumentator import Instrumentator
from shared.monitoring_thread import MonitoringThread
from shared.node_state import FederatedNodeState
from shared.resource_guard import get_resource_guard
from shared.shared_main import shared_router
from cloud.communication.cloud_main import CloudMain, cloud_router

app = FastAPI()
cloud_main = CloudMain()

cloud_router.websocket('/ws')(cloud_main.websocket_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(shared_router, prefix="/node")
app.include_router(cloud_router, prefix="/cloud")

# instrument & expose /metrics on the same HTTP server (8081)
Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

# Start resource guard daemon for this process (cloud role)
RESOURCE_GUARD = get_resource_guard(role="cloud")


@app.on_event("startup")
async def startup_event():
    logger.info("Initializing cloud application...")
    monitoring_thread = None

    def start_listeners_when_node_is_ready(already_started=None):
        if already_started is None:
            already_started = {'done': False}
        node = FederatedNodeState.get_current_node()
        if not already_started['done'] and node is not None:
            logger.info("FederatedNodeState initialized! Stating AMQP/MQTT listeners...")
            cloud_main.coordinator.start_background_consumers()
            logger.info("Cloud startup complete.")
            already_started['done'] = True
            monitoring_thread.stop()

    monitoring_thread = MonitoringThread(
        start_listeners_when_node_is_ready,
        10
    )
    monitoring_thread.start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
