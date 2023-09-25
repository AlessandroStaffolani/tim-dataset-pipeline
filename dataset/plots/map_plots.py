import os.path
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.io as pio

from dataset.utils import ROOT_DIR, create_directory_from_filepath

MAPBOX_STYLE = 'carto-positron'

pio.renderers.default = "browser"


def plot_grid(
        df: pd.DataFrame,
        geojson: dict,
        center: dict,
        id_name='cellId',
        zoom=11.4,
        save_path: Optional[str] = None,
        title=None
):
    fig = px.choropleth_mapbox(df, geojson=geojson, locations=id_name,
                               color='internet',
                               color_continuous_scale="Greens",
                               range_color=(0, 0),
                               zoom=zoom, mapbox_style=MAPBOX_STYLE,
                               center=center, opacity=0.3, height=1250, width=1600
                               )
    fig.update_layout(margin={"r": 5, "t": 5, "l": 5, "b": 5})
    if title is not None:
        fig.update_layout(title_text=title, title_x=0.5, title_y=0.99)
    if save_path is not None:
        file_path = os.path.join(ROOT_DIR, save_path)
        create_directory_from_filepath(file_path)
        fig.write_image(file_path)
    fig.show()


def plot_map_scatter(
        df: pd.DataFrame,
        lat: str,
        lon: str,
        zoom=11.2,
        save_path: Optional[str] = None,
        hover_name=None,
        hover_data=None,
        title=None
):
    fig = px.scatter_mapbox(df, lat=lat, lon=lon, zoom=zoom, mapbox_style=MAPBOX_STYLE,
                            hover_name=hover_name, hover_data=hover_data, height=1250, width=1250)
    fig.update_layout(margin={"r": 5, "t": 5, "l": 5, "b": 5})
    if title is not None:
        fig.update_layout(title_text=title, title_x=0.5, title_y=0.9)
    if save_path is not None:
        file_path = os.path.join(ROOT_DIR, save_path)
        create_directory_from_filepath(file_path)
        fig.write_image(file_path)
    fig.show()


def plot_grid_with_scatter(
        grid_df: pd.DataFrame,
        grid_geojson: dict,
        scatter_df: pd.DataFrame,
        lat: str,
        lon: str,
        center: dict,
        id_name='cellId',
        zoom=11.4,
        save_path: Optional[str] = None,
        title=None,
):
    fig = px.choropleth_mapbox(grid_df, geojson=grid_geojson, locations=id_name,
                               color='internet',
                               color_continuous_scale="Greens",
                               range_color=(0, 0),
                               zoom=zoom, mapbox_style=MAPBOX_STYLE,
                               center=center, opacity=0.3, height=1250, width=1600
                               )
    fig.add_scattermapbox(
        lat=scatter_df[lat], lon=scatter_df[lon],
        mode='markers',
        text=scatter_df['text']
    )
    fig.update_layout(margin={"r": 5, "t": 5, "l": 5, "b": 5})
    if title is not None:
        fig.update_layout(title_text=title, title_x=0.5, title_y=0.99)
    if save_path is not None:
        file_path = os.path.join(ROOT_DIR, save_path)
        create_directory_from_filepath(file_path)
        fig.write_image(file_path)
    fig.show()


def plot_grid_with_scatter_aggregated(
        grid_df: pd.DataFrame,
        grid_geojson: dict,
        scatter_df: pd.DataFrame,
        lat: str,
        lon: str,
        center: dict,
        id_name='cellId',
        zoom=11.4,
        save_path: Optional[str] = None,
        title=None,
        scatter_df_size: Optional[str] = None
):
    fig = px.scatter_mapbox(
        data_frame=scatter_df,
        lat=lat,
        lon=lon,
        size=scatter_df_size,
        size_max=25,
        zoom=zoom, mapbox_style=MAPBOX_STYLE,
        center=center, height=1250, width=1600,
        hover_name='text'
    )
    # it would be nice to change the color of the choropleth
    # fig.add_choroplethmapbox(
    #     geojson=grid_geojson,
    #     locations=grid_df[id_name],
    #     z=grid_df['internet'],
    #     zauto=True
    # )
    fig.update_layout(margin={"r": 5, "t": 5, "l": 5, "b": 5})
    if title is not None:
        fig.update_layout(title_text=title, title_x=0.5, title_y=0.99)
    if save_path is not None:
        file_path = os.path.join(ROOT_DIR, save_path)
        create_directory_from_filepath(file_path)
        fig.write_image(file_path)
    fig.show()
