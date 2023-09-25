import json
import os.path

import pandas as pd

from dataset.downloader.http_download import download_dataset_chunk
from dataset.preprocessing.aggregate_bs_to_cell import aggregate_bs_single_chunk
from dataset.preprocessing.dataframe import load_dataset_chunk
from dataset.utils import ROOT_DIR, create_directory


def process_chunks(args):
    metadata_path = args.input
    output_folder = args.output
    chunks_to_process = args.n_chunks
    chunks_to_skip = args.skip
    server_url = args.server_url
    protocol = args.protocol
    bs_aggregation_step = args.bs_aggregation_step
    aggregated_bs_file = args.aggregated_bs_file
    full_aggregation = args.full_aggregation
    skip_download = args.skip_download
    aggregated_bs_file = os.path.join(ROOT_DIR, aggregated_bs_file)
    full_download_folder = os.path.join(ROOT_DIR, output_folder, 'full-chunks')
    full_out_folder = os.path.join(ROOT_DIR, output_folder, 'processed-chunks')
    aggregated_out_folder = os.path.join(ROOT_DIR, output_folder, 'aggregated-chunks')
    create_directory(full_download_folder)
    create_directory(full_out_folder)
    create_directory(aggregated_out_folder)
    print(f'INFO | Metadata path is: {metadata_path}')
    print(f'INFO | Saving path is: {output_folder}')
    print(f'INFO | Chunks to skip: {chunks_to_skip} | '
          f'Chunks to download is: {chunks_to_process} | server_url: {server_url} | protocol: {protocol} '
          f'| bs_aggregation_step: {bs_aggregation_step} | aggregated_bs_file: {aggregated_bs_file} '
          f'| full_aggregation: {full_aggregation} | skip download: {skip_download}')

    # read the metadata
    with open(os.path.join(ROOT_DIR, metadata_path), 'r') as f:
        metadata = json.load(f)

    files = metadata['datasetVersion']['files']
    n = chunks_to_process if chunks_to_process != -1 else len(files)
    print(f'INFO | Found {len(files)} dataset chunks, going to load {n - chunks_to_skip} chunks')

    aggregated_bs_df = None
    if bs_aggregation_step:
        aggregated_bs_df = pd.read_csv(aggregated_bs_file)

    for i in range(chunks_to_skip, n):
        # for each chunk of the dataset
        file = files[i]
        persistentId = file['dataFile']['persistentId']
        filename = file['dataFile']['filename']
        if not skip_download:
            # download the chunk and temporary save if
            file_path = download_dataset_chunk(server_url, persistentId, full_download_folder, filename,
                                               protocol=protocol)
            # load the chunk and do its preprocessing:
            # filtering unnecessary fields and grouping the data by hour, weed day and cell id
            chunk_df = load_dataset_chunk(file_path, keep_all_columns=False)
            print(f'INFO | Processing file chunk {i+1}/{n}', end='\r')
        filename_csv = filename
        filename_csv = filename_csv.replace('.txt', '.csv').replace('sms-call-internet-mi', 'internet-mi')
        # save the processed dataframe
        processed_chunk_path = os.path.join(full_out_folder, filename_csv)
        if not skip_download:
            chunk_df.to_csv(
                path_or_buf=processed_chunk_path,
                header=True,
                index=False
            )
        # aggregate the base stations to the cells, if requested
        if bs_aggregation_step:
            filename_aggregated = filename_csv.replace('internet-mi', 'aggregated-internet-mi')
            aggregated_chunk_path = os.path.join(aggregated_out_folder, filename_aggregated)
            aggregate_bs_single_chunk(
                chunk_path=processed_chunk_path,
                aggregated_bs_df=aggregated_bs_df,
                save_path=aggregated_chunk_path,
                keep_all_columns=full_aggregation
            )
        # remove the chunk file
        if not skip_download:
            os.remove(file_path)
        print(f'INFO |  Processed file chunk {i+1}/{n}')
    os.rmdir(full_download_folder)
    print(f'INFO | All the {n} chunks have been downloaded in {full_out_folder}')
