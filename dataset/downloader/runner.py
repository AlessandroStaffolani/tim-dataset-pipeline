import os

from dataset.downloader.http_download import download_dataset_chunk
from dataset.utils import load_json_file


def download_dataset(args):
    metadata_path = args.input
    save_folder = args.output
    chunks_to_load = args.n_chunks
    chunks_to_skip = args.skip
    server_url = args.server_url
    protocol = args.protocol
    print(f'INFO | Metadata path is: {metadata_path}')
    print(f'INFO | Saving path is: {save_folder}')
    print(f'INFO | Chunks to skip: {chunks_to_skip} | '
          f'Chunks to download is: {chunks_to_load} | server_url: {server_url} | protocol: {protocol}')
    if os.path.exists(metadata_path):
        metadata = load_json_file(metadata_path)
        files = metadata['datasetVersion']['files']
        n = chunks_to_load if chunks_to_load != -1 else len(files)
        print(f'INFO | Found {len(files)} dataset chunks, going to load {n - chunks_to_skip} chunks')
        for i in range(chunks_to_skip, n):
            file = files[i]
            persistentId = file['dataFile']['persistentId']
            filename = file['dataFile']['filename']
            download_dataset_chunk(server_url, persistentId, save_folder, filename, protocol=protocol)
        print(f'INFO | All the {n} chunks have been downloaded in {save_folder}')
    else:
        print(f'ERROR | Metadata path not exist: "{metadata_path}"')
        return None
