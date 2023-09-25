import json
import os
import time
from typing import Optional, List, Tuple

import pandas as pd
import requests

from dataset.utils import create_directory, format_bytes, print_status, ROOT_DIR
from dataset.geo_utils import get_box_features, get_box_min_max_coordinates, points_distance


def download_dataset_chunk(
        server_url: str,
        persistent_id: str,
        save_folder: str,
        filename: Optional[str] = None,
        protocol: str = 'https'
):
    filename = filename if filename is not None else f'{persistent_id.split("/")[-1]}.txt'
    if not os.path.isabs(save_folder):
        save_folder = os.path.join(ROOT_DIR, save_folder)
    full_path = os.path.join(save_folder, filename)
    full_uri = f'{protocol}://{server_url}/api/access/datafile/:persistentId?persistentId={persistent_id}'
    with (requests.get(full_uri, stream=True)) as r:
        if r.status_code == 200:
            total_len = int(r.headers['content-length'])
            formatted_total_len = format_bytes(total_len)
            create_directory(save_folder)
            downloaded = 0
            with open(full_path, 'wb') as file:
                for data in r.iter_content(chunk_size=1024*64):
                    downloaded += len(data)
                    file.write(data)
                    print_status(downloaded, total_len,
                                 pre_message=f'INFO | Downloading {filename}',
                                 loading_len=50,
                                 current_formatted=format_bytes(downloaded),
                                 total_formatted=formatted_total_len)
                print()
            return full_path
        else:
            print(f'ERROR | Error while collecting file: {persistent_id}')
            print(f'ERROR | Status code {r.status_code}')
            print(f'ERROR | Content {r.text}')


def download_base_stations(
        geojson: dict,
        api_path: str,
        api_token: str,
        save_path: str,
        box_side: int = 5,
        sleep_interval: float = 0
):
    base_stations = []
    base_stations_map = {}
    features = geojson['features']
    grid_side_len = int((len(features))**(1/2))
    count = 0
    for i in range(0, grid_side_len, box_side):
        for j in range(0, grid_side_len, box_side):
            box_features = get_box_features(i, j, span=box_side, side_len=grid_side_len, features=features)
            min_lon, min_lat, max_lon, max_lat = get_box_min_max_coordinates(box_features)
            bbox = f'bbox={min_lon},{min_lat},{max_lon},{max_lat}'
            full_url = f'{api_path}?{api_token}{bbox}'
            loaded_bs = _load_base_stations(full_url)
            if loaded_bs is not None:
                for bs in loaded_bs:
                    cell_id = bs['properties']['cell']
                    if cell_id not in base_stations_map:
                        base_stations.append(bs)
                        base_stations_map[cell_id] = True
            count += 1
            print_status(count, box_side ** 2, 'Loading base stations from box')
            time.sleep(sleep_interval)
    print()
    print(f'Loaded {len(base_stations)} base stations')
    base_stations_geojson = {
        'type': 'FeatureCollection',
        'features': base_stations
    }
    with open(os.path.join(ROOT_DIR, save_path), 'w') as f:
        json.dump(base_stations_geojson, f)


def _load_base_stations(url: str) -> dict:
    r = requests.get(url)
    if r.status_code == 200:
        base_stations = r.json()
        if 'status' in base_stations and base_stations['status'] == 'error':
            print(f'ERROR | url: {url} | message: {base_stations["message"]}')
        else:
            return base_stations['features']
    else:
        print(f'ERROR | Error while downloading using url: {url}')
