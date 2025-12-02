import json
import os
import time

from tempfile import NamedTemporaryFile

from cloud.communication.cloud_resources_paths import CloudResourcesPaths
from shared.logging_config import logger


class RoundState:
    def __init__(self):
        self.round_id: int | None = None
<<<<<<< HEAD
        self.date: str | None = None
        self.expected_fogs: list[str] | None = None
        self.downlink_fogs: list[str] | None = None
        os.makedirs(CloudResourcesPaths.STATUS_FOLDER_PATH.value, exist_ok=True)
        self.restore()

    def persist(self, round_id: int, date: str | None = None):
        path = CloudResourcesPaths.ROUND_FILE_PATH.value
        data = {"round_id": int(round_id), "date": date, "ts": int(time.time())}
        try:
            self.round_id = int(round_id)
            if date is not None:
                self.date = str(date)
=======
        os.makedirs(CloudResourcesPaths.STATUS_FOLDER_PATH.value, exist_ok=True)
        self.restore()

    def persist(self, round_id: int):
        path = CloudResourcesPaths.ROUND_FILE_PATH.value
        data = {"round_id": int(round_id), "ts": int(time.time())}
        try:
            self.round_id = int(round_id)
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af
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
<<<<<<< HEAD
                date = data.get("date")
                if round_id is not None:
                    self.round_id = int(round_id)
                    self.date = date
                    logger.info(f"[Cloud]: restoring round_id {round_id} from {path}")
        except Exception as e:
            logger.warning(f"[Cloud]: failed to restore round_id from {path}: {e}")

    def set_expected_fogs(self, fogs: list[str] | None):
        try:
            self.expected_fogs = list(fogs) if fogs else None
        except Exception:
            self.expected_fogs = None

    def set_downlink_fogs(self, fogs: list[str] | None):
        try:
            self.downlink_fogs = list(fogs) if fogs else None
        except Exception:
            self.downlink_fogs = None
=======
                if round_id is not None:
                    self.round_id = int(round_id)
                    logger.info(f"[Cloud]: restoring round_id {round_id} from {path}")
        except Exception as e:
            logger.warning(f"[Cloud]: failed to restore round_id from {path}: {e}")
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af
