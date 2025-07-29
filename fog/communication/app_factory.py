from shared.logging_config import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from shared.shared_main import shared_router
from fog.communication.fog_messaging import FogMessaging

app = FastAPI()

fog_messaging = FogMessaging()

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
    logger.info("Initializing fog application...")
    fog_messaging.start_mqtt_listener()
    fog_messaging.start_amqp_listener()
    fog_messaging.start_edge_model_listener()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)