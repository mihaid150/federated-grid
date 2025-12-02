import json, os, time
from tempfile import NamedTemporaryFile
from shared.logging_config import logger
from fog.communication.fog_resources_paths import FogResourcesPaths

class FogRoundState:
    def __init__(self):
        self.round_id: int | None = None
        self._outbox_enabled: bool = False
        os.makedirs(FogResourcesPaths.STATUS_FOLDER_PATH.value, exist_ok=True)
        self.restore()

    def enable_outbox(self, enabled: bool = True):
        self._outbox_enabled = bool(enabled)

    @property
    def outbox_enabled(self) -> bool:
        return self._outbox_enabled

    def persist(self, round_id: int):
        path = FogResourcesPaths.ROUND_FILE_PATH.value
        data = {"round_id": int(round_id), "ts": int(time.time())}
        try:
            self.round_id = int(round_id)
            with NamedTemporaryFile(mode="w", dir=os.path.dirname(path), delete=False) as temp_file:
                json.dump(data, temp_file); temp_file.flush(); os.fsync(temp_file.fileno())
            os.replace(temp_file.name, path)
        except Exception as e:
            logger.warning(f"[Fog]: Failed to persist round_id {round_id} to {path}: {e}")\

    def restore(self):
        path = FogResourcesPaths.ROUND_FILE_PATH.value
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    data = json.load(f)
                    round_id = data.get("round_id")
                    if round_id is not None:
                        self.round_id = int(round_id)
                        logger.info(f"[Fog]: Restoring round_id {round_id} from {path}.")
        except Exception as e:
            logger.warning(f"[Fog]: Failed to restore round_id from {path}: {e}")