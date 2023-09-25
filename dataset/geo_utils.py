from math import radians, cos, sin, asin, sqrt
from typing import List, Tuple

import pandas as pd


def is_point_in_cell(cell: dict, point: Tuple[float, float]) -> float:
    min_lng, min_lat, max_lng, max_lat = get_box_min_max_coordinates([cell])
    point_lng, point_lat = point
    return min_lng <= point_lng <= max_lng and min_lat <= point_lat <= max_lat


def get_box_features(row: int, col: int, span: int, side_len: int, features: List[dict]) -> List[dict]:
    box_features = []
    for i in range(row * side_len, (row + span) * side_len, side_len):
        for j in range(col, col + span):
            cell_index = i + j
            # if i > 0:
            #     cell_index -= 1
            box_features.append(features[cell_index])
    return box_features


def get_box_min_max_coordinates(box_features: List[dict]) -> Tuple[float, float, float, float]:
    columns = ['lon', 'lat']
    data = []
    for feature in box_features:
        coordinates = feature['geometry']['coordinates'][0]
        for cor in coordinates:
            data.append([cor[0], cor[1]])
    df = pd.DataFrame(data=data, columns=columns)
    min_lon = df['lon'].min()
    min_lat = df['lat'].min()
    max_lon = df['lon'].max()
    max_lat = df['lat'].max()
    return min_lon, min_lat, max_lon, max_lat


def get_cell_center(cell: dict) -> Tuple[float, float]:
    columns = ['lng', 'lat']
    data = []
    for cord in cell['geometry']['coordinates'][0]:
        data.append([cord[0], cord[1]])
    df = pd.DataFrame(data=data, columns=columns)
    center_lng = df['lng'].mean()
    center_lat = df['lat'].mean()
    return center_lng, center_lat


def points_distance(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    return haversine_distance(a, b)


def haversine_distance(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [a[0], a[1], b[0], b[1]])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r


def get_feature_lng_and_lat(feature: dict) -> Tuple[float, float]:
    geo_type = feature['geometry']['type']
    coordinates = feature['geometry']['coordinates']
    if geo_type == 'Point':
        return coordinates[0], coordinates[1]
    elif geo_type == 'MultiPoint':
        return coordinates[0][0], coordinates[0][1]
