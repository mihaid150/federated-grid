from enum import Enum


class FogResourcesPaths(str, Enum):
    MODELS_FOLDER_PATH = "/app/models/"
    FOG_MODEL_FILE_PATH = MODELS_FOLDER_PATH + "fog_model.keras"
    OUTBOX_FOLDER_PATH = MODELS_FOLDER_PATH + "outbox/"

    STATUS_FOLDER_PATH = "/app/status/"
    ROUND_FILE_PATH = STATUS_FOLDER_PATH + "round.json"

