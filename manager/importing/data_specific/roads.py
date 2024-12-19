import geopandas as gpd
import numpy as np 
import pandas as pd
from shapely import wkt

def preprocess_roads_df(df):
    df['nodes'] = df['nodes'].apply(lambda x: list(eval(x)))

    df['geometry'] = df['wkt'].apply(wkt.loads)

    df['coordinates'] = df['geometry'].apply(lambda geom: list(geom.coords))

    def match_ids_to_values(ids, values):
        unique_ids = iter(ids)
        value_to_id = {}

        for value in values:
            if value not in value_to_id:
                value_to_id[value] = next(unique_ids)

        return value_to_id

    def close_nodes_to_coordinates(row):
        if len(row['nodes']) == len(row['coordinates']):
            return row
        if row['coordinates'][0] == row['coordinates'][-1]:  #
            row['nodes'].append(row['nodes'][0])
        if len(row['nodes']) != len(row['coordinates']):
            result = match_ids_to_values(row['nodes'], row['coordinates'])
            row['nodes'] = [result[coordinate] for coordinate in row['coordinates']]
        return row

    df = df.apply(close_nodes_to_coordinates, axis=1)

    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry='geometry')
    gdf['long'] = gdf.centroid.x
    gdf['lat'] = gdf.centroid.y

    df_roads = gdf.drop(['wkt', 'geometry', 'coordinates'], axis=1)
    return df_roads, df

#id	name	road_class	lanes	width	oneway	nodes	long	lat
def create_roads_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Node:Road {{
            id: row.id,
            name: row.name, 
            road_class: row.road_class,
            lanes: row.lanes,
            width: row.width,
            oneway: row.oneway,
            nodes: row.nodes,
            lat: ToFloat(row.lat),
            lng: ToFloat(row.long)
        }})"""

def preprocess_road_node_df(df):
    df_nodes = df[['nodes', 'coordinates']]
    df_nodes['nodes'] = df_nodes['nodes'].apply(list)
    df_nodes = df_nodes.explode(['nodes', 'coordinates'])
    df_nodes = pd.DataFrame({
        'id': df_nodes['nodes'].astype(int), 
        'wkt': df_nodes['coordinates'].apply(lambda coord: f"POINT ({coord[0]} {coord[1]})")
    }).reset_index(drop=True)

    df_nodes['geometry'] = df_nodes['wkt'].apply(wkt.loads)

    gdf_nodes = gpd.GeoDataFrame(df_nodes, crs="EPSG:4326", geometry='geometry')
    gdf_nodes['long'] = gdf_nodes.geometry.x
    gdf_nodes['lat'] = gdf_nodes.geometry.y

    gdf_nodes.to_crs(epsg=2180, inplace=True)
    gdf_nodes['x'] = gdf_nodes.geometry.x
    gdf_nodes['y'] = gdf_nodes.geometry.y
    gdf_nodes = gdf_nodes.drop(['geometry', 'wkt'], axis=1)
    gdf_nodes.drop_duplicates(subset="id", inplace=True)
    return gdf_nodes

# id	long	lat	x	y
def create_road_node_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Node:RoadNode {{
            id: row.id,
            lat: ToFloat(row.lat),
            lng: ToFloat(row.long),
            geometry:point({{x: ToFloat(row.x), y: ToFloat(row.y)}}) 
        }})"""
