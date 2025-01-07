import geopandas as gpd
import numpy as np 
import pandas as pd
from shapely import wkt

def preprocess_trees_df(df):
    crs = {'init': 'epsg:4326'}
    df['geometry'] = df['wkt'].apply(wkt.loads)
    
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry='geometry')
    gdf['lat'] = gdf.geometry.y
    gdf['long'] = gdf.geometry.x
    
    gdf.to_crs(epsg=2180, inplace=True)
    gdf['x'] = gdf.geometry.x
    
    gdf['y'] = gdf.geometry.y
    gdf = gdf.drop(['geometry', 'wkt'], axis=1)
    return gdf

def create_trees_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Node:Tree {{
            id: toInteger(row.id),
            lat: toFloat(row.lat),
            lng: toFloat(row.long),
            geometry: point({{x: toFloat(row.x), y: toFloat(row.y)}}) 
        }})"""

def create_tree_label_index_query():
    return "CREATE INDEX ON :Tree"

def create_tree_point_index_query():
    return "CREATE POINT INDEX ON :Tree(geometry)"