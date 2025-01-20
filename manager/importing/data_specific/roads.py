import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import Point


def preprocess_roads_df(df):
    df.fillna({"oneway": "no"}, inplace=True)
    df["nodes"] = df["nodes"].apply(lambda x: list(eval("[" + x[1:-1] + "]")))
    df["start_node_id"] = df["nodes"].apply(lambda x: x[0])
    df["end_node_id"] = df["nodes"].apply(lambda x: x[-1])
    df["geometry"] = df["wkt"].apply(wkt.loads)
    df["coordinates"] = df["geometry"].apply(lambda geom: list(geom.coords))

    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry="geometry")
    gdf["long"] = gdf.centroid.x
    gdf["lat"] = gdf.centroid.y

    gdf.to_crs(epsg=2180, inplace=True)
    gdf["wkt"] = gdf["geometry"].apply(wkt.dumps)
    gdf = pd.concat([gdf, gdf.bounds], axis=1)
    df_roads = gdf.drop(["geometry", "coordinates", "nodes"], axis=1)
    return df_roads, df


# id	name	road_class	lanes	width	oneway  start_node_id   end_node_id	long	lat minx	miny	maxx	maxy
def create_roads_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:Road {{
            id: toInteger(row.id),
            name: row.name, 
            road_class: row.road_class,
            lanes: row.lanes,
            width: row.width,
            oneway: row.oneway,
            start_node_id: toInteger(row.start_node_id),
            end_node_id: toInteger(row.end_node_id),
            wkt: row.wkt,
            lat: toFloat(row.lat),
            lng: toFloat(row.long),
            lower_left_corner: point({{x: toFloat(row.minx), y: toFloat(row.miny)}}),
            upper_right_corner: point({{x: toFloat(row.maxx), y: toFloat(row.maxy)}})
        }})"""


def preprocess_road_node_df(df):
    df_nodes = df[["id", "nodes", "coordinates"]]
    df_nodes["road_id"] = df_nodes["id"]
    df_nodes = df_nodes.explode(["nodes", "coordinates"])

    df_nodes["id"] = df_nodes["nodes"].astype(int)
    df_nodes["geometry"] = df_nodes["coordinates"].apply(
        lambda coord: Point(coord[0], coord[1])
    )

    gdf_nodes = gpd.GeoDataFrame(
        df_nodes[["id", "geometry", "road_id"]], crs="EPSG:4326"
    )

    gdf_nodes["long"] = gdf_nodes.geometry.x
    gdf_nodes["lat"] = gdf_nodes.geometry.y

    gdf_nodes = gdf_nodes.to_crs(epsg=2180)
    gdf_nodes["x"] = gdf_nodes.geometry.x
    gdf_nodes["y"] = gdf_nodes.geometry.y

    gdf_nodes = gdf_nodes.drop(["geometry"], axis=1)
    return gdf_nodes


# id	road_id	long	lat	x	y
def create_road_node_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        CREATE (n:RoadNode {{
            id: toInteger(row.id),
            lat: toFloat(row.lat),
            lng: toFloat(row.long),
            geometry: point({{x: toFloat(row.x), y: toFloat(row.y)}}) 
        }})
        """


# id	road_id
def create_road_node_road_connection_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row 
        MATCH (r:Road {{id: toInteger(row.road_id)}}), (n:RoadNode {{id: toInteger(row.id)}})
        CREATE (n)-[:BELONGS_TO]->(r)
        """


def preprocess_road_node_connections(df):
    df.fillna({"oneway": "no"}, inplace=True)
    exploded_df = df[["oneway", "nodes", "id"]].explode("nodes")
    exploded_df["node_end"] = exploded_df["nodes"].shift(-1)
    exploded_df["id_end"] = exploded_df["id"].shift(-1)
    exploded_df = exploded_df[exploded_df["id"] == exploded_df["id_end"]]
    exploded_df = exploded_df.drop(columns=["id", "id_end"])

    exploded_df = exploded_df.dropna(subset=["node_end"])
    exploded_df["node_start"] = exploded_df["nodes"]
    exploded_df = exploded_df.drop(columns=["nodes"])
    reverse_df = exploded_df[exploded_df["oneway"] == "no"].copy()
    reverse_df[["node_start", "node_end"]] = reverse_df[
        ["node_end", "node_start"]
    ].values

    road_node_connetions_df = pd.concat([exploded_df, reverse_df], ignore_index=True)
    del exploded_df
    del reverse_df
    road_node_connetions_df.drop(columns=["oneway"], inplace=True)
    road_node_connetions_df.drop_duplicates(
        subset=["node_end", "node_start"], inplace=True
    )
    return road_node_connetions_df


# node_end	node_start
def create_road_node_connection_input_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (startNode:RoadNode {{id: toInteger(row.node_start)}}), (endNode:RoadNode {{id: toInteger(row.node_end)}})
        MERGE (startNode)-[:CONNECTED_TO {{distance: point.distance(startNode.geometry, endNode.geometry)}}]->(endNode)
    """
