import pandas as pd
from shared.logging_config import logger


def filter_data_by_interval_date(file_path: str, filtering_column_name: str, start_date: str, end_date: str,
                                 output_file_path: str, print_loggings: bool = True):
    # if print_loggings:
        # logger.info("Starting filter_data_by_interval_date function")
        # logger.info(f"Parameters received: file_path={file_path}, filtering_column_name={filtering_column_name}, "
        #            f"start_date={start_date}, end_date={end_date}, output_file_path={output_file_path}")
    chunk_size = 10000
    is_first_chunk = True

    try:
        chunk_num = 0
        start_date_parsed = pd.to_datetime(start_date)
        end_date_parsed = pd.to_datetime(end_date)
        # if print_loggings:
            # logger.info(f"Parsed start_date: {start_date_parsed} and end_date: {end_date_parsed}")

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk_num += 1
            # if print_loggings:
                # logger.info(f"Processing chunk {chunk_num} with {len(chunk)} rows")
                # logger.info(f"Chunk {chunk_num}: Columns found: {list(chunk.columns)}")

            # convert the filtering column to datetime, ignoring errors
            chunk[filtering_column_name] = pd.to_datetime(chunk[filtering_column_name], errors='coerce')
            # if print_loggings:
                # logger.info(f"Chunk {chunk_num}: Converted column '{filtering_column_name}' to datetime. "
                #             f"Data type now: {chunk[filtering_column_name].dtype}")

            nat_count = chunk[filtering_column_name].isna().sum()
            # if print_loggings:
                # logger.info(f"Chunk {chunk_num}: Found {nat_count} NaT values in column '{filtering_column_name}'")

            # if print_loggings:
                # logger.info(f"Filtering chunk {chunk_num} between {start_date_parsed} (inclusive) "
                #             f"and {end_date_parsed} (exclusive)")
            filtered_chunk = chunk[(chunk[filtering_column_name] >= start_date_parsed) &
                                   (chunk[filtering_column_name] < end_date_parsed)]
            # if print_loggings:
                # logger.info(f"Chunk {chunk_num}: {len(filtered_chunk)} rows remain after filtering")

            if not filtered_chunk.empty:
                filtered_chunk.to_csv(output_file_path, mode='a', index=False, header=is_first_chunk)
                # if print_loggings:
                #     logger.info(f"Chunk {chunk_num}: Appended filtered data to {output_file_path}")
                is_first_chunk = False

        logger.info("Completed processing all chunks successfully.")
        return output_file_path

    except Exception as e:
        logger.error(f"An error occurred in filter_data_by_interval_date: {e}")
        return None
