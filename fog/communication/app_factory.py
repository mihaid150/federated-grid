from shared.logging_config import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import threading
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
    threading.Thread(target=fog_messaging.start_mqtt_listener, daemon=True).start()
    threading.Thread(target=fog_messaging.start_amqp_listener, daemon=True).start()
    threading.Thread(target=fog_messaging.start_edge_model_listener, daemon=True).start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)