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
            id: row.id,
            lat: ToFloat(row.lat),
            lng: ToFloat(row.long),
            geometry:point({{x: ToFloat(row.x), y: ToFloat(row.y)}}) 
        }})"""
