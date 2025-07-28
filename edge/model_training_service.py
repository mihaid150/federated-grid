import os
import pandas as pd
import numpy as np
import tensorflow as tf
from edge.data_preprocessing import preprocess_data
from edge.data_selection import filter_data_by_interval_date
from shared.logging_config import logger
from edge.edge_resources_paths import EdgeResourcesPaths
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


def data_generator(file_path, feature_columns, target_column, sequence_length):
    for chunk in pd.read_csv(file_path, chunksize=5000):
        chunk = chunk.dropna(subset=[target_column])
        X = chunk[feature_columns].astype('float32').values
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


def train_local_edge_model(training_date: str, sequence_length: int = 144):

    # define training and evaluation intervals
    training_day1 = training_date
    training_day2 = (pd.to_datetime(training_date) + pd.Timedelta(days=2)).strftime("%Y-%m-%d")
    evaluation_day1 = (pd.to_datetime(training_date) + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    evaluation_day2 = (pd.to_datetime(training_date) + pd.Timedelta(days=5)).strftime("%Y-%m-%d")
    logger.info(
        f"Training local edge non_trained_local_edge_model with training day1 {training_day1}, training day2 {training_day2}, "
        f"evaluation day1 {evaluation_day1} and evaluation day2 {evaluation_day2}.")

    training_data_path = EdgeResourcesPaths.TRAINING_DAYS_DATA_PATH
    evaluation_data_path = EdgeResourcesPaths.EVALUATION_DAYS_DATA_PATH

    # filter and preprocess training data
    filter_data_by_interval_date(EdgeResourcesPaths.INPUT_DATA_PATH, "datetime", training_day1,
                                 training_day2, training_data_path)
    preprocess_data(training_data_path, "datetime", "apparent power (kWh)")
    post_preprocessing_padding(training_data_path, sequence_length)
    train_data = pd.read_csv(training_data_path)
    logger.info(f"Training data shape for 2 days is {train_data.shape}")

    # filter and preprocess evaluation data
    filter_data_by_interval_date(EdgeResourcesPaths.INPUT_DATA_PATH, "datatime", evaluation_day1,
                                 evaluation_day2, evaluation_data_path)
    preprocess_data(evaluation_data_path, "datetime", "apparent power (kWh)")
    post_preprocessing_padding(evaluation_data_path, sequence_length)
    evaluation_data = pd.read_csv(evaluation_data_path)
    logger.info(f"Evaluation data shape for 2 days is {evaluation_data.shape}")

    # define feature columns
    feature_columns = required_columns.copy()
    feature_columns.remove("value")

    # build streaming datasets from the generator
    try:
        autotune = tf.data.experimental.AUTOTUNE
    except AttributeError:
        # fallback: prefetch a small fixed number of batches
        autotune = 1

    train_dataset = tf.data.Dataset.from_generator(
        lambda: data_generator(training_data_path, feature_columns, 'value', sequence_length),
        output_types=(tf.float32, tf.float32),
        output_shapes=(
            tf.TensorShape([None, sequence_length, len(feature_columns)]),
            tf.TensorShape([None])
        )
    ).prefetch(autotune)

    evaluation_dataset = tf.data.Dataset.from_generator(
        lambda: data_generator(evaluation_data_path, feature_columns, 'value', sequence_length),
        output_types=(tf.float32, tf.float32),
        output_shapes=(
            tf.TensorShape([None, sequence_length, len(feature_columns)]),
            tf.TensorShape([None])
        )
    ).prefetch(autotune)

    # load and evaluate the pre-trained model before fine-tuning
    custom_objects = {
        "LogCosh": tf.keras.losses.LogCosh(),
        "mse": tf.keras.losses.MeanSquaredError(),
        "Huber": tf.keras.losses.Huber()
    }

    non_trained_local_edge_model = tf.keras.models.load_model(EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH,
                                                              custom_objects=custom_objects)

    # compute baseline metrics
    y_true_before, y_pred_before = [], []
    for X_batch, y_batch in evaluation_dataset:
        predictions = non_trained_local_edge_model.predict(X_batch)
        y_true_before.append(y_batch.numpy())
        y_pred_before.append(predictions)
    y_true_before = np.concatenate(y_true_before)
    y_pred_before = np.concatenate(y_pred_before)
    evaluation_before = compute_metrics(y_true_before, y_pred_before)
    logger.info(f"Metrics before retraining: {evaluation_before}")

    # compile and train the model using the streaming dataset
    optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
    non_trained_local_edge_model.compile(optimizer=optimizer, loss=tf.keras.losses.Huber())
    early_stopping = tf.keras.callbacks.EarlyStopping(
        monitor='loss', patience=5, restore_best_weights=True
    )
    logger.info("Retraining the non_trained_local_edge_model on streaming dataset...")
    non_trained_local_edge_model.fit(train_dataset, epochs=10, callbacks=[early_stopping], verbose=1)

    # evaluate the model after fine‑tuning
    y_true_after, y_pred_after = [], []
    for X_batch, y_batch in evaluation_dataset:
        predictions = non_trained_local_edge_model.predict(X_batch)
        y_true_after.append(y_batch.numpy())
        y_pred_after.append(predictions)
    y_true_after = np.concatenate(y_true_after)
    y_pred_after = np.concatenate(y_pred_after)
    evaluation_after = compute_metrics(y_true_after, y_pred_after)
    logger.info(f"Metrics after retraining: {evaluation_after}")

    # save the fine‑tuned model
    trained_edge_model_file_path = os.path.join(
        EdgeResourcesPaths.MODELS_FOLDER_PATH,
        EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH,
    )
    non_trained_local_edge_model.save(trained_edge_model_file_path)
    logger.info(f"Trained non_trained_local_edge_model saved at: {trained_edge_model_file_path}")

    return {
        "before_training": evaluation_before,
        "after_training": evaluation_after,
    }
