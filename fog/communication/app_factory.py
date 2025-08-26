from shared.logging_config import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import threading

from shared.monitoring_thread import MonitoringThread
from shared.node_state import FederatedNodeState
from shared.shared_main import shared_router
from fog.communication.fog_messaging import FogMessaging
app = FastAPI()

fog_messaging = FogMessaging()

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
    logger.info("Initializing fog application...")
    monitoring_thread = None

    def start_listeners_when_node_is_ready(fog_messaging_arg, already_started=None):
        if already_started is None:
            already_started = {'done': False}
        node = FederatedNodeState.get_current_node()
        if not already_started['done'] and node is not None:
            logger.info("FederatedNodeState initialized! Stating AMQP/MQTT listeners...")
            threading.Thread(target=fog_messaging_arg.start_mqtt_listener, daemon=True).start()
            threading.Thread(target=fog_messaging_arg.start_amqp_listener, daemon=True).start()
            threading.Thread(target=fog_messaging_arg.start_edge_model_listener, daemon=True).start()
            threading.Thread(target=fog_messaging_arg.start_cloud_uplink_worker, daemon=True).start()
            already_started['done'] = True
            monitoring_thread.stop()

    monitoring_thread = MonitoringThread(
        start_listeners_when_node_is_ready,
        10,
        fog_messaging,
    )
    monitoring_thread.start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)