import base64
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING
from shared.logging_config import logger
from edge.communication.edge_resources_paths import EdgeResourcesPaths
from edge.model.model_architectures import create_model
from edge.model.model_training_service import train_local_edge_model
from shared.resource_guard import get_resource_guard


if TYPE_CHECKING:
    from edge.communication.coordinator import EdgeCoordinator


class EdgeService:
    def __init__(self, coordinator: 'EdgeCoordinator'):
        self.edge_coordinator = coordinator
        self._resource_guard = get_resource_guard(role="edge")


    @staticmethod
    def create_local_edge_model():
        edge_model = create_model('simple_lstm_two_gates')
        Path(EdgeResourcesPaths.MODELS_FOLDER_PATH.value).mkdir(parents=True, exist_ok=True)
        local_edge_model_path = EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        edge_model.save(local_edge_model_path)
        logger.info(f"[Edge]: Successfully created and saved local edge model.")


    def train_edge_local_model(self, payload):
        """
        Train local edge model.
        Accepts a variety of payload shapes:
          - dict with {'data': {'date': 'YYYY-MM-DD'}}
          - dict with {'date': 'YYYY-MM-DD'}
          - dict with {'params': {'date': 'YYYY-MM-DD'}}
          - raw date string 'YYYY-MM-DD'
        """
        params = {}
        if isinstance(payload, dict):
            params = payload.get('params') or payload.get('data') or payload
        elif isinstance(payload, str):
            params = {"date": payload}

        # Allow nested edge params under 'edge'
        edge_params = params.get('edge') if isinstance(params, dict) else None
        if isinstance(edge_params, dict):
            merged = dict(params)
            merged.update(edge_params)
            params = merged

        date = params.get('date')
        seq_len = int(params.get('sequence_length', 144))
        epochs = int(params.get('epochs', 10))
        logger.info("[Edge]: training request params date=%s seq_len=%s epochs=%s raw_params=%s", date, seq_len, epochs, params)
        self._resource_guard.wait_for_capacity("edge-training-start")
        # optional feature selection hints propagated via params
        try:
            if 'feature_top_k' in params:
                os.environ['EDGE_TOP_K_FEATURES'] = str(int(params.get('feature_top_k')))
            if 'feature_strict_mask' in params:
                os.environ['EDGE_FEATURE_STRICT_MASK'] = '1' if str(params.get('feature_strict_mask')).lower() in ('1',
                                                                                                                   'true',
                                                                                                                   'yes') else '0'
            if 'must_have_features' in params and isinstance(params.get('must_have_features'), (list, tuple)):
                os.environ['EDGE_FEATURES_MUST_HAVE'] = ','.join(str(x) for x in params.get('must_have_features'))
            logger.info("[Edge]: feature selection env EDGE_TOP_K_FEATURES=%s EDGE_FEATURE_STRICT_MASK=%s EDGE_FEATURES_MUST_HAVE=%s",
                        os.environ.get('EDGE_TOP_K_FEATURES'), os.environ.get('EDGE_FEATURE_STRICT_MASK'), os.environ.get('EDGE_FEATURES_MUST_HAVE'))
        except Exception:
            pass
        metrics = train_local_edge_model(
            date,
            sequence_length=seq_len,
            epochs=epochs,
            resource_guard=self._resource_guard,
        )
        local_edge_model_path = EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        self.edge_coordinator.send_trained_model(local_edge_model_path, metrics)


    def retrain_fog_model(self, msg):
        local_edge_model_path = EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        model_bytes = base64.b64decode(msg['model'])
        with open(local_edge_model_path, "wb") as f:
            f.write(model_bytes)
        date = msg.get('data', {}).get('date') if isinstance(msg, dict) else None
        logger.info("[Edge]: retrain_fog_model date=%s model_bytes=%d", date, len(model_bytes) if isinstance(model_bytes, (bytes, bytearray)) else -1)
        self._resource_guard.wait_for_capacity("edge-retrain-start")
        metrics = train_local_edge_model(date, resource_guard=self._resource_guard)
        trained_edge_model_file_path = EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        self.edge_coordinator.send_trained_model(trained_edge_model_file_path, metrics)


    def handle_agent_nudge(self, nudge):
        """Accept different agent-nudge envelope shapes and call the training entrypoint with the extracted date/params.
        Errors are logged and swallowed to avoid crashing the MQTT handler."""
        try:
            # normalize payload: support {"params": ...}, {"payload": ...} or raw dict
            params = {}
            if isinstance(nudge, dict):
                params = nudge.get("params") or nudge.get("payload") or nudge
            else:
                # try json string
                try:
                    params = json.loads(nudge)
                except Exception:
                    params = {"value": nudge}

            # require a date somewhere in params
            date = None
            if isinstance(params, dict):
                date = params.get("date") or (params.get("data") or {}).get("date")
                if not date and isinstance(params.get("edge"), dict):
                    date = params["edge"].get("date")

            if not date:
                logger.info("[Edge]: agent nudge missing 'date'; ignoring nudge: %s", nudge)
                return

            try:
                self.train_edge_local_model({"params": params})
            except Exception as e:
                logger.error("[Edge]: failed handling agent nudge: %s", e, exc_info=True)
        except Exception as e:
            logger.error("[Edge]: failed handling agent nudge: %s", e, exc_info=True)
