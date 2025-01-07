import geopandas as gpd
import numpy as np 
import pandas as pd
from shapely import wkt

def preprocess_buildings_df(df):
    df['geometry'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry='geometry')
    gdf['lat'] = gdf.centroid.x
    gdf['long'] = gdf.centroid.y

    gdf.to_crs(epsg=2180, inplace=True)
    gdf['x'] = gdf.centroid.x
    gdf['y'] = gdf.centroid.y
    gdf = pd.concat([gdf, gdf.bounds], axis=1)
    gdf['wkt'] = gdf['geometry'].apply(wkt.dumps)
    gdf['radius'] = gdf.apply(lambda row: np.sqrt((row['x'] - row['maxx'])** 2 + (row['y'] - row['maxy'])** 2), axis=1)
    gdf = gdf.drop(['geometry', 'minx', 'miny', 'maxx', 'maxy'], axis=1)
    return gdf

# id	building	wkt	lat	long	x	y	radius
def create_buildings_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Node:Building {{
            id: toInteger(row.id),
            building: row.building,
            lat: toFloat(row.lat),
            lng: toFloat(row.long),
            wkt: row.wkt, 
            center: point({{x: toFloat(row.x), y: toFloat(row.y)}}) ,
            radius: toFloat(row.radius)
        }})"""

def create_building_label_index_query():
    return "CREATE INDEX ON :Building"

def create_building_id_index_query():
    return "CREATE INDEX ON :Building(id)"

def create_building_center_point_index_query():
    return "CREATE POINT INDEX ON :Building(center)"
