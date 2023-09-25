import json
import os
import datetime
from typing import Optional, Tuple

import pandas as pd

from dataset.geo_utils import get_feature_lng_and_lat
from dataset.utils import ROOT_DIR


def date_parser(date):
    return datetime.datetime.fromtimestamp(float(date) / 1000)


FIELDS = ['cellId', 'datetime', 'countrycode', 'smsin', 'smsout', 'callin', 'callout', 'internet']
DATE_FIELDS = ['datetime']
DF_COLUMNS = ['cellId', 'datetime', 'internet']


def load_dataset_chunk(
        file_path: str,
        sep: str = '\t',
        encoding='utf-8-sig',
        keep_all_columns=False,
) -> pd.DataFrame:
    if os.path.exists(file_path):
        df = pd.read_csv(file_path,
                         sep=sep,
                         encoding=encoding,
                         names=FIELDS,
                         parse_dates=DATE_FIELDS,
                         date_parser=date_parser)
        df = df.set_index('datetime')
        df['hour'] = df.index.hour
        df['weekday'] = df.index.weekday
        df = df.groupby(['hour', 'weekday', 'cellId'], as_index=False).sum()
        df['idx'] = df['hour'] + (df['weekday'] * 24)
        if not keep_all_columns:
            columns_to_remove = [col for col in FIELDS if col not in DF_COLUMNS]
            df.drop(columns=columns_to_remove, inplace=True)
        return df
    else:
        raise AttributeError(f'ERROR | File path provided not exists: {file_path}')


def generate_empty_grid_dataset(geojson_path: str, save_path: str):
    with open(os.path.join(ROOT_DIR, geojson_path), 'r') as f:
        geojson = json.load(f)

    columns = ['cellId', 'internet']
    data = []
    for feature in geojson['features']:
        cellId = feature['properties']['cellId']
        data.append([cellId, 0])

    df = pd.DataFrame(data=data, columns=columns)
    df.to_csv(
        path_or_buf=os.path.join(ROOT_DIR, save_path),
        index=False
    )


def geojson_base_stations_to_df(geojson_path: str, filter_type: Optional[Tuple[str, ...]] = None) -> pd.DataFrame:
    columns = ['bs_id', 'type', 'range', 'lat', 'lng']
    data = []
    with open(os.path.join(ROOT_DIR, geojson_path), 'r') as f:
        geojson = json.load(f)

    for feature in geojson['features']:
        coordinates = get_feature_lng_and_lat(feature)
        properties = feature['properties']
        bs_type = properties['radio']
        if filter_type is None or bs_type in filter_type:
            data.append([
                properties['cell'],
                bs_type,
                properties['range'],
                coordinates[1],
                coordinates[0]
            ])

    return pd.DataFrame(data=data, columns=columns)

