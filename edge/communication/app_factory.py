from shared.logging_config import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from shared.shared_main import shared_router
from edge.communication.edge_messaging import EdgeMessaging, EdgeService


app = FastAPI()

edge_messaging = EdgeMessaging()
edge_service = EdgeService(edge_messaging)
edge_messaging.edge_service = edge_service

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credential=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(shared_router, prefix="/node")


@app.on_event("startup")
async def startup_event():
    logger.info("Initializing edge application...")
    edge_messaging.start_amqp_listener()
    edge_messaging.start_mqtt_listener()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)