import geopandas as gpd
import numpy as np 
import pandas as pd
from shapely import wkt

def preprocess_powiats_df(df):
    df['geometry'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry='geometry')
    gdf['lat'] = gdf.centroid.y
    gdf['long'] = gdf.centroid.x

    gdf.to_crs(epsg=2180, inplace=True)
    gdf = pd.concat([gdf, gdf.bounds], axis=1)
    gdf['wkt'] = gdf['geometry'].apply(wkt.dumps)
    gdf = gdf.drop(['geometry'], axis=1)
    return gdf

# id	name	wkt	lat	long	minx	miny	maxx	maxy
def create_powiats_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Node:Powiat {{
            id: row.id,
            name: row.name,
            lat: ToFloat(row.lat),
            lng: ToFloat(row.long),
            geometry: row.wkt, 
            lower_left_corner: point({{x: ToFloat(row.minx), y: ToFloat(row.miny)}}),
            upper_right_corner: point({{x: ToFloat(row.maxx), y: ToFloat(row.maxy)}})
        }})"""
