import os.path
from glob import glob

import numpy as np
import pandas as pd

from dataset.utils import ROOT_DIR, create_directory_from_filepath, print_status


def aggregate_bs_to_cell(
        chunks_folder: str,
        aggregated_bs_path: str,
        save_path: str,
        mode: str = 'a',
        keep_all_columns: bool = False
):
    chunks_folder = os.path.join(ROOT_DIR, chunks_folder)
    aggregated_bs_path = os.path.join(ROOT_DIR, aggregated_bs_path)
    save_path = os.path.join(ROOT_DIR, save_path)
    create_directory_from_filepath(save_path)
    chunks = glob(f'{chunks_folder}/*.csv')

    aggregated_bs_df = pd.read_csv(aggregated_bs_path)
    sliced_columns = ['cellId', 'aggregated_bs_id']
    if keep_all_columns:
        sliced_columns = ['cellId', 'aggregated_bs_id', 'type', 'lng', 'lat', 'n_base_stations']

    for i, file_path in enumerate(chunks):
        chunk_df = pd.read_csv(file_path)
        sliced = aggregated_bs_df[sliced_columns]
        chunk_df = pd.merge(chunk_df, sliced, on=['cellId'])
        columns_reordered = ['hour', 'weekday', 'idx', 'internet']
        columns_groupby = ['hour', 'weekday', 'idx']
        for col in sliced_columns:
            if col != 'cellId':
                columns_reordered.append(col)
                columns_groupby.append(col)
        chunk_df = chunk_df[columns_reordered]
        chunk_df = chunk_df.groupby(columns_groupby, as_index=False).sum()
        if mode == 'a':
            keep_header = True if i == 0 else False
            save_path_full = save_path
        else:
            keep_header = True
            save_path_full = save_path.replace('.csv', f'-{get_date_from_file_path(file_path)}.csv')

        # ids = []
        # for j in range(2850, 2850 + 2000, 70):
        #     start = j
        #     ids += np.arange(start, start+15).tolist()
        #
        # chunk_df = chunk_df[chunk_df['aggregated_bs_id'].isin(ids)]

        chunk_df.to_csv(
            path_or_buf=save_path_full, header=keep_header, mode=mode, index=False
        )
        print_status(i+1, len(chunks), 'Processed chunks', loading_len=40)
    print()


def aggregate_bs_single_chunk(
        chunk_path: str,
        aggregated_bs_df: pd.DataFrame,
        save_path: str,
        keep_all_columns: bool = False
):
    sliced_columns = ['cellId', 'aggregated_bs_id']
    if keep_all_columns:
        sliced_columns = ['cellId', 'aggregated_bs_id', 'type', 'lng', 'lat', 'n_base_stations']
    chunk_df = pd.read_csv(chunk_path)

    sliced = aggregated_bs_df[sliced_columns]
    chunk_df = pd.merge(chunk_df, sliced, on=['cellId'])
    columns_reordered = ['hour', 'weekday', 'idx', 'internet']
    columns_groupby = ['hour', 'weekday', 'idx']
    for col in sliced_columns:
        if col != 'cellId':
            columns_reordered.append(col)
            columns_groupby.append(col)
    chunk_df = chunk_df[columns_reordered]
    chunk_df = chunk_df.groupby(columns_groupby, as_index=False).sum()

    chunk_df.to_csv(path_or_buf=save_path, header=True, index=False)


def get_date_from_file_path(file_path: str) -> str:
    last_item = file_path.split('/')[-1]
    last_item = last_item.replace('.csv', '')
    date_portion = last_item.split('-')[-3:]
    return '-'.join(date_portion)

