import os
import json

import numpy as np
import pandas as pd

from dataset.plots.map_plots import plot_grid, plot_map_scatter, plot_grid_with_scatter, \
    plot_grid_with_scatter_aggregated
from dataset.preprocessing.dataframe import geojson_base_stations_to_df
from dataset.utils import ROOT_DIR


def plot_empty_grid_on_map(geojson_path: str, empty_grid_path: str, center, zoom=11.4, save_path=None, title=None):
    with open(os.path.join(ROOT_DIR, geojson_path), 'r') as f:
        geojson = json.load(f)

    df = pd.read_csv(empty_grid_path)

    plot_grid(df, geojson, id_name='cellId', center=center, zoom=zoom, save_path=save_path, title=title)


def plot_base_stations_on_map(geojson_path: str, filter_type=None, save_path=None, title=None):
    df = geojson_base_stations_to_df(geojson_path, filter_type=filter_type)
    df['text'] = f'Type: {df["type"]} - Range: {df["range"]}'
    plot_map_scatter(df, lat='lat', lon='lng', zoom=11.2, save_path=save_path,
                     hover_name='bs_id', hover_data=['type', 'range'], title=title)


def plot_base_stations_with_grid(
        grid_geojson_path: str,
        empty_grid_path: str,
        bs_geojson_path: str,
        center, zoom=11.4, save_path=None, title=None, filter_type=None,
):
    with open(os.path.join(ROOT_DIR, grid_geojson_path), 'r') as f:
        grid_geojson = json.load(f)

    grid_df = pd.read_csv(empty_grid_path)
    bs_df = geojson_base_stations_to_df(bs_geojson_path, filter_type=filter_type)
    bs_df['text'] = 'Id: ' + bs_df['bs_id'].apply(str) + ' - Type: ' + bs_df['type'].apply(str) + ' - Range: ' + bs_df['range'].apply(str)

    plot_grid_with_scatter(
        grid_df=grid_df,
        grid_geojson=grid_geojson,
        scatter_df=bs_df,
        lat='lat',
        lon='lng',
        center=center,
        zoom=zoom,
        save_path=save_path,
        title=title
    )


def plot_base_stations_with_grid_aggregated(
        grid_geojson_path: str,
        empty_grid_path: str,
        bs_df_path: str,
        center, zoom=11.4, save_path=None, title=None,
):
    with open(os.path.join(ROOT_DIR, grid_geojson_path), 'r') as f:
        grid_geojson = json.load(f)

    grid_df = pd.read_csv(empty_grid_path)
    bs_df = pd.read_csv(bs_df_path)
    bs_df['text'] = 'Aggregated BS Id: ' + bs_df['aggregated_bs_id'].apply(str) + ' - Type: ' + bs_df['type'].apply(str) + ' - N base stations: ' + bs_df['n_base_stations'].apply(str)

    plot_grid_with_scatter_aggregated(
        grid_df=grid_df,
        grid_geojson=grid_geojson,
        scatter_df=bs_df,
        lat='lat',
        lon='lng',
        center=center,
        zoom=zoom,
        save_path=save_path,
        title=title,
        scatter_df_size='n_base_stations'
    )
