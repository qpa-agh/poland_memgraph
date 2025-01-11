import geopandas as gpd
import numpy as np 
import pandas as pd
from shapely import wkt

def preprocess_communes_df(df):
    df['geometry'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry='geometry')
    gdf['lat'] = gdf.centroid.y
    gdf['long'] = gdf.centroid.x

    gdf.to_crs(epsg=2180, inplace=True)
    gdf['x'] = gdf.centroid.x
    gdf['y'] = gdf.centroid.y
    gdf = pd.concat([gdf, gdf.bounds], axis=1)
    gdf['wkt'] = gdf['geometry'].apply(wkt.dumps)
    gdf = gdf.drop(['geometry'], axis=1)
    return gdf

# id	name	wkt	lat	long	x   y   minx	miny	maxx	maxy
def create_communes_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Node:Commune {{
            id: toInteger(row.id),
            name: row.name,
            lat: toFloat(row.lat),
            lng: toFloat(row.long),
            wkt: row.wkt, 
            center: point({{x: toFloat(row.x), y: toFloat(row.y)}}) ,
            lower_left_corner: point({{x: toFloat(row.minx), y: toFloat(row.miny)}}),
            upper_right_corner: point({{x: toFloat(row.maxx), y: toFloat(row.maxy)}})
        }})"""

