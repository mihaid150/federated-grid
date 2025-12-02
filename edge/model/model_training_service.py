from pathlib import Path

import pandas as pd
import numpy as np
import os
import tensorflow as tf
from edge.model.data_preprocessing import preprocess_data
from edge.model.data_selection import filter_data_by_interval_date
from shared.logging_config import logger
from edge.communication.edge_resources_paths import EdgeResourcesPaths
from shared.resource_guard import get_resource_guard
from shared.utils import required_columns
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from tensorflow.keras.preprocessing import timeseries_dataset_from_array


def compute_metrics(y_true, y_pred):
    mse_val = float(mean_squared_error(y_true, y_pred))
    mae_val = float(mean_absolute_error(y_true, y_pred))
    r2_val = float(r2_score(y_true, y_pred))
    logcosh_val = float(np.mean(np.log(np.cosh(y_pred - y_true))))

    huber_loss_fn = tf.keras.losses.Huber()
    huber_val = float(huber_loss_fn(y_true, y_pred).numpy())

    # msle: ensure no negative values by using log1p
    msle_val = float(np.mean((np.log1p(y_true) - np.log1p(y_pred)) ** 2))

    return {
        "mse": mse_val,
        "mae": mae_val,
        "r2": r2_val,
        "logcosh": logcosh_val,
        "huber": huber_val,
        "msle": msle_val
    }


def post_preprocessing_padding(data_file_path: str, required_length: int, mask_value: float = -1):
    df = pd.read_csv(data_file_path)
    current_rows = len(df)
    if required_length > current_rows > 0:
        last_row = df.iloc[-1].copy()
        last_row["synthetic"] = True  # mark as synthetic if needed
        missing = 2 * required_length - current_rows
        synthetic_rows = [last_row.copy() for _ in range(missing)]
        df_synthetic = pd.DataFrame(synthetic_rows)
        df = pd.concat([df, df_synthetic], ignore_index=True)
        df.to_csv(data_file_path, index=False)
    return data_file_path


def _compute_feature_weights(df: pd.DataFrame, feature_columns: list[str], target_column: str = 'value') -> dict[str, float]:
    """
    Compute simple relevance weights per feature using absolute Pearson correlation with the target
    Always keep a minimum floor weight for must-have features
    Configurable via env:
        - EDGE_FEATURES_MUST_HAVE: comma list; defaults to 'drift_flag,time_since_last_spike'
        - EDGE_TOP_K_FEATURES: integer; if set and EDGE_FEATURE_STRICT_MASK=true, non-top-K features get weight 0.
        - EDGE_FEATURE_STRICT_MASK: 'true'|'false' (default false) zeroes out non-top-K features.
    """

    must_have = [s.strip() for s in os.environ.get("EDGE_FEATURES_MUST_HAVE", 'drift_flag,time_since_last_spike').split(',') if s.strip()]
    strict_mask = os.getenv('EDGE_FEATURE_STRICT_MASK', 'false').lower() in ('1', 'true', 'yes')
    try:
        top_k = int(os.getenv("EDGE_TOP_K_FEATURES", '0'))
        if top_k <= 0:
            top_k = None  # no top-k limitation
    except Exception:
        top_k = None

    # compute absolute correlation; fall back to 0 if invalid
    weights: dict[str, float] = {}
    target = df[target_column].astype('float32')
    for column in feature_columns:
        try:
            c = float(abs(df[column].astype('float32').corr(target)))
            if np.isnan(c):
                c = 0.0
        except Exception:
            c = 0.0
        weights[column] = c

    # rank and optionally mask to top-k ensuring must-have present
    if top_k is not None and top_k > 0:
        # make sure must-have are included even if correlation is low
        sorted_cols = sorted(feature_columns, key=lambda f: weights.get(f, 0.0), reverse=True)
        chosen = []
        for f in sorted_cols:
            if f in must_have and f not in chosen:
                chosen.append(f)
        for f in sorted_cols:
            if len(chosen) >= top_k:
                break
            if f not in chosen:
                chosen.append(f)
        chosen = chosen[:top_k]
        if strict_mask:
            for f in feature_columns:
                if f not in chosen:
                    weights[f] = 0.0
        else:
            # softly downweight non-chosen
            min_w = max(0.05, min(weights.values() or [0.1]))
            for f in feature_columns:
                if f not in chosen:
                    weights[f] = min_w
    # enforce a floor for must-have features
    for f in must_have:
        if f in weights:
            weights[f] = max(weights[f], 0.3)

    # normalize weights to [0,1]
    mx = max(weights.values() or [0.1])
    if mx > 0:
        for k in list(weights.keys()):
            weights[k] = float(weights[k] / mx)

    return weights


def _build_windows(df: pd.DataFrame, feature_columns: list[str], target_column: str, sequence_length: int,
                   col_weights: dict[str, float] | None = None) -> tuple[np.ndarray, np.ndarray]:
    X = df[feature_columns].astype('float32').values
    if col_weights:
        # scale columns by weights
        w = np.array([col_weights.get(c, 1.0) for c in feature_columns], dtype=np.float32)
        X = X * w[None, :]
    y = df[target_column].astype('float32').values
    n = len(df)
    if n < sequence_length:
        return np.empty((0, sequence_length, len(feature_columns)), dtype=np.float32),np.empty((0,),dtype=np.float32)
    num = n - sequence_length + 1
    X_win = np.zeros((num, sequence_length, X.shape[1]), dtype=np.float32)
    for i in range(num):
        X_win[i] = X[i:i + sequence_length]
    y_lab = y[sequence_length - 1:]
    return X_win, y_lab

def data_generator(file_path, feature_columns, target_column, sequence_length, col_weights: dict[str, float] | None = None):
    for chunk in pd.read_csv(file_path, chunksize=5000):
        chunk = chunk.dropna(subset=[target_column])
        X = chunk[feature_columns].astype('float32').values
        if col_weights:
            w = np.array([col_weights.get(c, 1.0) for c in feature_columns], dtype=np.float32)
            X = X * w[None, :]
        y = chunk[target_column].astype('float32').values

        dataset = timeseries_dataset_from_array(
            data=X,
            targets=y,
            sequence_length=sequence_length,
            sequence_stride=1,
            batch_size=32,
            shuffle=False
        )

        for batch in dataset:
            yield batch


class _ResourceGuardCallback(tf.keras.callbacks.Callback):
    def __init__(self, guard, reason: str, check_every_batches: int = 10):
        super().__init__()
        self._guard = guard
        self._reason = reason
        self._check_batches = max(0, int(check_every_batches))

    def on_epoch_begin(self, epoch, logs=None):
        self._guard.wait_for_capacity(f"{self._reason}-epoch-{epoch}")

    def on_train_batch_begin(self, batch, logs=None):
        if self._check_batches and batch % self._check_batches == 0:
            self._guard.wait_for_capacity(f"{self._reason}-batch-{batch}")


def train_local_edge_model(
    training_date: str,
    sequence_length: int = 144,
    batch_size: int = 32,
    epochs: int = 10,
    *,
    resource_guard=None,
):
    guard = resource_guard or get_resource_guard(role="edge")
    guard.wait_for_capacity("edge-training-prep")
    # --- date handling: parse safely & normalize to ISO ---
    start_dt = pd.to_datetime(training_date, dayfirst=True, errors="coerce")
    if pd.isna(start_dt):
        raise ValueError(f"Could not parse training_date={training_date!r}")
    training_day1 = start_dt.strftime("%Y-%m-%d")
    training_day2 = (start_dt + pd.Timedelta(days=2)).strftime("%Y-%m-%d")
    evaluation_day1 = (start_dt + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    evaluation_day2 = (start_dt + pd.Timedelta(days=5)).strftime("%Y-%m-%d")

    logger.info(
        f"Training local edge non_trained_local_edge_model with training day1 {training_day1}, "
        f"training day2 {training_day2}, evaluation day1 {evaluation_day1} and "
        f"evaluation day2 {evaluation_day2}."
    )

    training_data_path = EdgeResourcesPaths.TRAINING_DAYS_DATA_PATH.value
    evaluation_data_path = EdgeResourcesPaths.EVALUATION_DAYS_DATA_PATH.value

    # --- filter & preprocess ---
    filter_data_by_interval_date(EdgeResourcesPaths.INPUT_DATA_PATH.value, "datetime",
                                 training_day1, training_day2, training_data_path)
    preprocess_data(training_data_path, "datetime", "apparent power (kWh)")
    post_preprocessing_padding(training_data_path, sequence_length)
    train_df = pd.read_csv(training_data_path)
    logger.info(f"Training data shape for 2 days is {train_df.shape}")

    filter_data_by_interval_date(EdgeResourcesPaths.INPUT_DATA_PATH.value, "datetime",
                                 evaluation_day1, evaluation_day2, evaluation_data_path)
    preprocess_data(evaluation_data_path, "datetime", "apparent power (kWh)")
    post_preprocessing_padding(evaluation_data_path, sequence_length)
    eval_df = pd.read_csv(evaluation_data_path)
    logger.info(f"Evaluation data shape for 2 days is {eval_df.shape}")

    # --- features ---
    all_features = required_columns.copy()
    # Target column is 'value'; remove it from feature list if present
    if "value" in all_features:
        all_features.remove("value")

    # compute static relevance weights from training data
    col_weights = _compute_feature_weights(train_df, all_features, target_column='value')
    feature_columns = all_features
    logger.info("Edge: feature weights computed (top-5): %s",
                sorted([(k, round(v, 3)) for k, v in col_weights.items()], key=lambda x: x[1], reverse=True)[:5])

    # --- streaming datasets ---
    try:
        autotune = tf.data.experimental.AUTOTUNE
    except AttributeError:
        autotune = 1

    def make_ds(csv_path):
        return tf.data.Dataset.from_generator(
            lambda: data_generator(csv_path, feature_columns, 'value', sequence_length, col_weights),
            output_types=(tf.float32, tf.float32),
            output_shapes=(
                tf.TensorShape([None, sequence_length, len(feature_columns)]),
                tf.TensorShape([None])
            ),
        )

    train_dataset = make_ds(training_data_path).prefetch(autotune)
    evaluation_dataset = make_ds(evaluation_data_path).prefetch(autotune)

    # --- compute steps_per_epoch deterministically ---
    def num_steps(df_len):
        sequences = max(0, df_len - sequence_length + 1)
        return max(1, int(np.ceil(sequences / batch_size)))

    train_steps = num_steps(len(train_df))
    eval_steps = num_steps(len(eval_df))

    # repeat train dataset to prevent "ran out of data"
    train_dataset = train_dataset.repeat()
    evaluation_dataset_eval = evaluation_dataset.take(eval_steps)

    # --- load model ---
    custom_objects = {
        "LogCosh": tf.keras.losses.LogCosh(),
        "mse": tf.keras.losses.MeanSquaredError(),
        "Huber": tf.keras.losses.Huber()
    }
    non_trained_local_edge_model = tf.keras.models.load_model(
        EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value,
        custom_objects=custom_objects
    )

    # Align requested sequence_length with model's input shape to avoid shape mismatches
    try:
        model_seq_len = int(non_trained_local_edge_model.input_shape[1])
        if model_seq_len != int(sequence_length):
            logger.info(
                "Requested sequence_length=%s differs from model's input=%s; clamping to model input.",
                sequence_length, model_seq_len
            )
            sequence_length = model_seq_len
    except Exception:
        pass

    # --- baseline metrics (windowed, also compute conditional spike/baseline) ---
    X_eval, y_eval = _build_windows(eval_df, feature_columns, 'value', sequence_length, col_weights)
    y_pred_before = non_trained_local_edge_model.predict(X_eval, verbose=0) if len(X_eval) else np.array([])
    evaluation_before = compute_metrics(y_eval, y_pred_before) if len(y_eval) else {k: float('nan') for k in
                                                                                    ('mse', 'mae', 'r2', 'logcosh',
                                                                                     'huber', 'msle')}
    logger.info(f"Metrics before retraining: {evaluation_before}")

    # --- train with validation ---
    optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
    non_trained_local_edge_model.compile(optimizer=optimizer, loss=tf.keras.losses.Huber())
    early_stopping = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
    guard_callback = _ResourceGuardCallback(guard, reason="edge-training", check_every_batches=10)

    guard.wait_for_capacity("edge-training-fit")
    logger.info("Retraining the non_trained_local_edge_model on streaming dataset...")
    non_trained_local_edge_model.fit(
        train_dataset,
        epochs=int(epochs),
        steps_per_epoch=train_steps,
        validation_data=evaluation_dataset_eval,
        validation_steps=eval_steps,
        callbacks=[early_stopping, guard_callback],
        verbose=1
    )

    # --- post-train metrics (windowed, conditional) ---
    y_pred_after = non_trained_local_edge_model.predict(X_eval, verbose=0) if len(X_eval) else np.array([])
    evaluation_after = compute_metrics(y_eval, y_pred_after) if len(y_eval) else {k: float('nan') for k in
                                                                                  ('mse', 'mae', 'r2', 'logcosh',
                                                                                   'huber', 'msle')}
    logger.info(f"Metrics after retraining: {evaluation_after}")

    # Conditional error split by regime (baseline vs spike at label time)
    try:
        drift_flags = eval_df['drift_flag'].astype(int).values
        drift_targets = drift_flags[sequence_length - 1:sequence_length - 1 + len(y_eval)]
        mask_spike = (drift_targets == 1)
        mask_base = (drift_targets == 0)
        cond_metrics = {}
        if mask_base.any():
            cond_metrics['baseline'] = compute_metrics(y_eval[mask_base], y_pred_after[mask_base])
        if mask_spike.any():
            cond_metrics['spike'] = compute_metrics(y_eval[mask_spike], y_pred_after[mask_spike])
    except Exception as e:
        logger.warning(f"Edge: failed conditional metrics split: {e}")
        cond_metrics = {}

    # --- save trained model ---
    guard.wait_for_capacity("edge-training-save")
    Path(EdgeResourcesPaths.MODELS_FOLDER_PATH.value).mkdir(parents=True, exist_ok=True)
    trained_edge_model_file_path = EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
    non_trained_local_edge_model.save(trained_edge_model_file_path, include_optimizer=False)
    logger.info(f"Trained non_trained_local_edge_model saved at: {trained_edge_model_file_path}")

    return {
        "before_training": evaluation_before,
        "after_training": evaluation_after,
        "conditional": cond_metrics,
    }
