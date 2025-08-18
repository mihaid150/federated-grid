from enum import Enum


class FogResourcesPaths(str, Enum):
    MODELS_FOLDER_PATH = "/app/models/"
    FOG_MODEL_FILE_PATH = MODELS_FOLDER_PATH + "fog_model.keras"
    OUTBOX_FOLDER_PATH = MODELS_FOLDER_PATH + "outbox/"


