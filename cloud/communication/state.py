import json
import os
import time

from tempfile import NamedTemporaryFile

from cloud.communication.cloud_resources_paths import CloudResourcesPaths
from shared.logging_config import logger


class RoundState:
    def __init__(self):
        self.round_id: int | None = None
        os.makedirs(CloudResourcesPaths.STATUS_FOLDER_PATH.value, exist_ok=True)
        self.restore()

    def persist(self, round_id: int):
        path = CloudResourcesPaths.ROUND_FILE_PATH.value
        data = {"round_id": int(round_id), "ts": int(time.time())}
        try:
            self.round_id = int(round_id)
            with NamedTemporaryFile("w", dir=os.path.dirname(path), delete=False) as temp_file:
                json.dump(data, temp_file)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                tmp = temp_file.name
            os.rename(tmp, path)
        except Exception as e:
            logger.warning(f"[Cloud]: failed to persist round_id to {path}: {e}")

    def restore(self):
        path = CloudResourcesPaths.ROUND_FILE_PATH.value
        try:
            if os.path.exists(path):
                with open(path) as file:
                    data = json.load(file)
                round_id = data.get("round_id")
                if round_id is not None:
                    self.round_id = int(round_id)
                    logger.info(f"[Cloud]: restoring round_id {round_id} from {path}")
        except Exception as e:
            logger.warning(f"[Cloud]: failed to restore round_id from {path}: {e}")