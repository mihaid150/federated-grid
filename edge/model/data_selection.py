import os
import pandas as pd
from shared.logging_config import logger


def filter_data_by_interval_date(file_path: str, filtering_column_name: str,
                                 start_date: str, end_date: str,
                                 output_file_path: str, print_loggings: bool = True):
    chunk_size = 10000
    is_first_chunk = True

    # NEW: start clean so every run overwrites previous results
    try:
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
    except Exception as e:
        logger.warning(f"Could not remove existing {output_file_path}: {e}")

    try:
        start_date_parsed = pd.to_datetime(start_date)
        end_date_parsed = pd.to_datetime(end_date)

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk[filtering_column_name] = pd.to_datetime(chunk[filtering_column_name], errors='coerce')
            filtered_chunk = chunk[(chunk[filtering_column_name] >= start_date_parsed) &
                                   (chunk[filtering_column_name] < end_date_parsed)]
            if not filtered_chunk.empty:
                mode = 'w' if is_first_chunk else 'a'
                filtered_chunk.to_csv(output_file_path, mode=mode, index=False, header=is_first_chunk)
                is_first_chunk = False

        logger.info("Completed processing all chunks successfully.")
        return output_file_path

    except Exception as e:
        logger.error(f"An error occurred in filter_data_by_interval_date: {e}")
        return None

