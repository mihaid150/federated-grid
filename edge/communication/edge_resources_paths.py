from enum import Enum


class EdgeResourcesPaths(str, Enum):
    MODELS_FOLDER_PATH = "/app/models/"
    NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH = MODELS_FOLDER_PATH + "non_trained_local_edge_model.keras"
    TRAINED_LOCAL_EDGE_MODEL_FILE_PATH = MODELS_FOLDER_PATH + "trained_local_edge_model.keras"

    DATA_FOLDER_PATH = "/app/data/"
    FILTERED_DATA_FOLDER_PATH = DATA_FOLDER_PATH + "/filtered_data/"
    INPUT_DATA_PATH = DATA_FOLDER_PATH + "input_data.csv"
    FILTERED_DATA_PATH = FILTERED_DATA_FOLDER_PATH + "filtered_data.csv"

    TRAINING_DAYS_DATA_PATH = FILTERED_DATA_FOLDER_PATH + "training_days_data.csv"
    EVALUATION_DAYS_DATA_PATH = FILTERED_DATA_FOLDER_PATH + "evaluation_days_data.csv"
