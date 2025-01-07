import geopandas as gpd
import numpy as np 
import pandas as pd
from shapely import wkt

def preprocess_railways_df(df):
    df['geometry'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry='geometry')
    gdf['lat'] = gdf.centroid.y
    gdf['long'] = gdf.centroid.x

    gdf.to_crs(epsg=2180, inplace=True)
    gdf = pd.concat([gdf, gdf.bounds], axis=1)
    gdf['wkt'] = gdf['geometry'].apply(wkt.dumps)
    gdf = gdf.drop(['geometry'], axis=1)
    return gdf

# id	railway	wkt	lat	long	minx	miny	maxx	maxy
def create_railways_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Node:Railway {{
            id: toInteger(row.id),
            railway: row.railway,
            lat: toFloat(row.lat),
            lng: toFloat(row.long),
            wkt: row.wkt,
            lower_left_corner: point({{x: toFloat(row.minx), y: toFloat(row.miny)}}),
            upper_right_corner: point({{x: toFloat(row.maxx), y: toFloat(row.maxy)}})
        }})"""

def create_railway_label_index_query():
    return "CREATE INDEX ON :Railway"