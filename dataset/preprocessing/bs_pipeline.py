import datetime
import math
import os.path
import time
from typing import List, Tuple, Dict

import pandas as pd
from pymongo import TEXT

from dataset.downloader.http_download import download_base_stations
from dataset.mongodb.geojson_uploader import upload_geojson
from dataset.mongodb.query import get_db, get_bs_in_range
from dataset.utils import load_json_file, ROOT_DIR, create_directory, print_status
from dataset.geo_utils import is_point_in_cell, get_cell_center, points_distance, get_feature_lng_and_lat


def process_base_stations(args):
    geojson_path = args.input
    save_folder = os.path.join(ROOT_DIR, args.output_folder)
    api_path = args.api_path
    api_token = args.api_token
    box_side = args.box_side
    sleep_interval = args.sleep_interval
    to_collection = args.collection
    skip_bs_download = args.skip_bs_download
    skip_db_upload = args.skip_db_upload
    bs_types = args.bs_types
    create_directory(save_folder)
    print(f'INFO | Geojson path: {geojson_path}')
    print(f'INFO | Save path: {save_folder}')
    print(f'INFO | Api path: {api_path} | box_side: {box_side} | sleep_interval: {sleep_interval}'
          f' | Saving in collection: {to_collection} | Skip BS download: {skip_bs_download} |'
          f' Skip DB upload: {skip_db_upload} | BS Types: {bs_types}')
    # load the grid dataset
    geojson = load_json_file(os.path.join(ROOT_DIR, geojson_path))
    # download all the base stations in a certain area, delimited by all the cells of the dataset grid
    full_bs_save_path = os.path.join(save_folder, 'bs_milan.geojson')
    # if not skip_bs_download:
    #     download_base_stations(geojson, api_path, api_token, full_bs_save_path, box_side, sleep_interval)
    if not skip_db_upload:
        upload_geojson(full_bs_save_path, to_collection=to_collection,
                       geosphere_index_name='geometry', additional_indexes=[('properties.radio', TEXT)])
    max_distance = 500  # in meters
    mongo_db = get_db()
    mapped_columns = ['bs_id', 'type', 'range', 'created', 'lng', 'lat', 'cellId', 'distance']
    mapped_data = []
    aggregated_columns = ['type', 'lng', 'lat', 'cellId', 'distance', 'n_base_stations', 'aggregated_bs_id']
    aggregated_data = []
    all_cells = geojson['features']
    n_cells = len(all_cells)
    aggregated_bs_id_mapping = {}
    for i, cell in enumerate(all_cells):
        set_cell_base_stations(cell, mapped_data, aggregated_data, mongo_db, to_collection, max_distance,
                               bs_types=bs_types, aggregated_bs_id_mapping=aggregated_bs_id_mapping)
        print_status(i+1, n_cells, 'Mapping grid cells to base stations', loading_len=50)
    print()
    cell_df = pd.DataFrame(data=mapped_data, columns=mapped_columns)
    # we store the cell_base_stations_mapped
    filename = f'cell_base_stations_mapped-{"-".join(bs_types.split(","))}.csv'
    cell_df.to_csv(
        path_or_buf=os.path.join(save_folder, filename),
        index=False
    )
    # we group by cellId so that we have one base station for cell,
    # composed by all the base station belonging to that cell, if any
    aggregated_df = pd.DataFrame(data=aggregated_data, columns=aggregated_columns)
    # we save the cell_base_stations_aggregated
    filename = f'cell_base_stations_aggregated-{"-".join(bs_types.split(","))}.csv'
    aggregated_df.to_csv(
        path_or_buf=os.path.join(save_folder, filename),
        index=False
    )

    bs_df = aggregated_df.groupby(['aggregated_bs_id', 'type', 'n_base_stations', 'lng', 'lat'], as_index=False).count()
    bs_df.drop(['cellId', 'distance'], 1, inplace=True)
    filename = f'aggregated_bs_data-{"-".join(bs_types.split(","))}.csv'
    bs_df.to_csv(path_or_buf=os.path.join(save_folder, filename), header=True, index=False)


def set_cell_base_stations(cell, mapped_data, aggregated_data, mongo_db, to_collection, max_distance,
                           aggregated_bs_id_mapping: Dict[(Tuple[float, float]), int],
                           max_retry=4, bs_types='LTE'):
    cell_center = get_cell_center(cell)
    cell_base_stations, distance = get_cell_base_stations(cell, cell_center, mongo_db, to_collection,
                                                          max_distance, bs_types)
    trials = 1
    while len(cell_base_stations) == 0 and trials <= max_retry:
        cell_base_stations, distance = get_cell_base_stations(
            cell, cell_center, mongo_db, to_collection, max_distance=max_distance * (2 * trials), bs_types=bs_types)
        trials += 1
    all_lng = []
    all_lat = []
    n_bs = 0
    bs_type = None
    for bs in cell_base_stations:
        bs_point = get_feature_lng_and_lat(bs)
        all_lng.append(bs_point[0])
        all_lat.append(bs_point[1])
        n_bs += 1
        bs_type = bs['properties']['radio']
        mapped_data.append([
            bs['properties']['cell'],
            bs['properties']['radio'],
            bs['properties']['range'],
            bs['properties']['created'],
            bs_point[0],
            bs_point[1],
            cell['properties']['cellId'],
            distance
        ])
    final_bs_type = bs_type if n_bs == 1 else 'AGGREGATED'
    avg_lng = sum(all_lng)/n_bs
    avg_lat = sum(all_lat)/n_bs
    aggregated_bs_id = get_aggregated_bs_id((avg_lng, avg_lat), aggregated_bs_id_mapping)
    aggregated_data.append([
        final_bs_type,
        avg_lng,
        avg_lat,
        cell['properties']['cellId'],
        distance,
        n_bs,
        aggregated_bs_id
    ])


def get_aggregated_bs_id(aggregated_bs_point: Tuple[float, float], aggregated_bs_id_mapping) -> int:
    if aggregated_bs_point not in aggregated_bs_id_mapping:
        aggregated_bs_id_mapping[aggregated_bs_point] = len(aggregated_bs_id_mapping) + 1
    return aggregated_bs_id_mapping[aggregated_bs_point]


def get_cell_base_stations(
        cell: dict,
        cell_center: Tuple[float, float],
        mongo_db,
        to_collection: str,
        max_distance: float,
        bs_types: str
) -> Tuple[List[dict], float]:
    base_stations = get_bs_in_range(
        to_collection,
        center=cell_center,
        max_distance=max_distance,
        filters={'properties.radio': {'$in': bs_types.split(',')}},
        db=mongo_db
    )
    bs_in_cell = False
    candidate_bs = []
    min_distance = math.inf
    count = 0
    count_after_min_found = None
    for bs in base_stations:
        bs_point = get_feature_lng_and_lat(bs)
        if is_point_in_cell(cell, bs_point):
            if bs_in_cell:
                candidate_bs.append(bs)
                count_after_min_found = 0
            else:
                bs_in_cell = True
                candidate_bs = [bs]
                min_distance = 0
                count_after_min_found = 0
        else:
            if not bs_in_cell:
                distance = points_distance(cell_center, bs_point)
                if distance < min_distance:
                    min_distance = distance
                    candidate_bs = [bs]
                    count_after_min_found = 0
            if count_after_min_found is not None:
                count_after_min_found += 1
        if count_after_min_found is not None and count_after_min_found > 10:
            return candidate_bs, min_distance
        count += 1
    return candidate_bs, min_distance
