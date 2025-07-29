from enum import Enum


class CloudResourcesPaths(str, Enum):
    MODELS_FOLDER_PATH = "/app/models/"
    CLOUD_MODEL_FILE_PATH = MODELS_FOLDER_PATH + "cloud_model.keras"


