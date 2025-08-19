from shared.logging_config import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import threading

from shared.node_state import FederatedNodeState
from shared.shared_main import shared_router
from edge.communication.edge_messaging import EdgeMessaging, EdgeService
from shared.monitoring_thread import MonitoringThread
from edge.agents.edge_agent import EdgeAgent

app = FastAPI()

edge_messaging = EdgeMessaging()
edge_service = EdgeService(edge_messaging)
edge_messaging.edge_service = edge_service


edge_agent = EdgeAgent()
edge_agent.attach_messaging(edge_messaging)
edge_messaging.attach_agent(edge_agent)

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

    def start_listeners_when_node_is_ready(edge_messaging_arg, already_started=None):
        if already_started is None:
            already_started = {'done': False}
        node = FederatedNodeState.get_current_node()
        if not already_started['done'] and node is not None:
            logger.info("FederatedNodeState initialized! Stating AMQP/MQTT listeners...")
            threading.Thread(target=edge_messaging_arg.start_amqp_listener, daemon=True).start()
            threading.Thread(target=edge_messaging_arg.start_mqtt_listener, daemon=True).start()
            edge_agent.start()
            already_started['done'] = True
            monitoring_thread.stop()

    monitoring_thread = MonitoringThread(
        start_listeners_when_node_is_ready,
        10,
        edge_messaging,
    )
    monitoring_thread.start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)