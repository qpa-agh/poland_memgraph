import geopandas as gpd
import numpy as np
import pandas as pd
import gc
from shapely import wkt, prepare
from shapely.geometry import Point, Polygon, LineString, MultiLineString

from pathlib import Path
import os
import shutil
import csv
from typing import Union

from utils.parallelization import execute_with_pool
from database.communication import (
    execute_query,
    execute_query_to_csv,
    execute_query_to_csv_parallelized,
)


def is_within(point: Point, line: Union[MultiLineString, LineString]) -> bool:
    if isinstance(line, MultiLineString):
        for subline in line.geoms:
            commune_polygon = Polygon(subline)
            if commune_polygon.contains(point):
                return True
    elif isinstance(line, LineString):
        return Polygon(line).contains(point)
    return False


def is_point_within_border(data):
    point_id, x, y, border_id, border_wkt = data
    point = Point(float(x), float(y))
    line = wkt.loads(border_wkt)
    if is_within(point, line):
        return [point_id, border_id]
    return None


def create_relationship_1():
    """
    Cities which are within commune boundaries
    """
    execute_query("CREATE POINT INDEX ON :City(center)")
    execute_query("CREATE INDEX ON :City(id)")
    execute_query("CREATE INDEX ON :Commune(id)")
    query = """
    MATCH (commune:Commune) 
    WITH  commune.lower_left_corner as llc, commune.upper_right_corner as urc, commune
    MATCH (city:City) 
    WHERE point.withinbbox(city.center, llc, urc)
    RETURN city.id AS city_id, city.center.x AS city_x, city.center.y AS city_y, commune.id AS commune_id, commune.wkt AS commune_wkt
    """
    headers = ["city_id", "commune_id"]
    output_file = "/data/city_commune_data.csv"
    execute_query_to_csv(
        query, headers, output_file, modifier_function=is_point_within_border
    )

    create_relationships_query = f"""
    LOAD CSV FROM '{output_file}' WITH HEADER AS row
    MATCH (city:City {{id: toInteger(row.city_id)}}), (commune:Commune {{id: toInteger(row.commune_id)}})
    CREATE (city)-[:LOCATED_IN]->(commune)
    """
    execute_query(create_relationships_query)
    execute_query("DROP POINT INDEX ON :City(center)")
    execute_query("DROP INDEX ON :City(id)")
    execute_query("DROP INDEX ON :Commune(id)")


def create_relationship_2():
    """
    Communes which are within powiat boundaries
    """
    execute_query("CREATE POINT INDEX ON :Commune(center)")
    execute_query("CREATE INDEX ON :Powiat(id)")
    execute_query("CREATE INDEX ON :Commune(id)")

    query = """
    MATCH (powiat:Powiat) 
    WITH  powiat.lower_left_corner as llc, powiat.upper_right_corner as urc, powiat
    MATCH (commune:Commune) 
    WHERE point.withinbbox(commune.center, llc, urc)
    RETURN commune.id AS commune_id, commune.center.x AS commune_x, commune.center.y AS commune_y, powiat.id AS powiat_id, powiat.wkt AS powiat_wkt
    """
    headers = ["commune_id", "powiat_id"]
    output_file = "/data/commune_powiat_data.csv"
    execute_query_to_csv(
        query, headers, output_file, modifier_function=is_point_within_border
    )

    create_relationships_query = f"""
    LOAD CSV FROM '{output_file}' WITH HEADER AS row
    MATCH (commune:Commune {{id: toInteger(row.commune_id)}}), (powiat:Powiat {{id: toInteger(row.powiat_id)}})
    CREATE (commune)-[:LOCATED_IN]->(powiat)
    """
    execute_query(create_relationships_query)

    execute_query("DROP POINT INDEX ON :Commune(center)")
    execute_query("DROP INDEX ON :Powiat(id)")
    execute_query("DROP INDEX ON :Commune(id)")


def create_relationship_3():
    """
    Powiats which are within voivodship boundaries
    """
    execute_query("CREATE POINT INDEX ON :Powiat(center)")
    execute_query("CREATE INDEX ON :Powiat(id)")
    execute_query("CREATE INDEX ON :Voivodship(id)")
    query = """
    MATCH (voivodship:Voivodship) 
    WITH  voivodship.lower_left_corner as llc, voivodship.upper_right_corner as urc, voivodship
    MATCH (powiat:Powiat) 
    WHERE point.withinbbox(powiat.center, llc, urc)
    RETURN powiat.id AS powiat_id, powiat.center.x AS powiat_x, powiat.center.y AS powiat_y, voivodship.id AS voivodship_id, voivodship.wkt AS voivodship_wkt
    """
    headers = ["powiat_id", "voivodship_id"]
    output_file = "/data/powiat_voivodship_data.csv"
    execute_query_to_csv(
        query, headers, output_file, modifier_function=is_point_within_border
    )

    create_relationships_query = f"""
    LOAD CSV FROM '{output_file}' WITH HEADER AS row
    MATCH (powiat:Powiat {{id: toInteger(row.powiat_id)}}), (voivodship:Voivodship {{id: toInteger(row.voivodship_id)}})
    CREATE (powiat)-[:LOCATED_IN]->(voivodship)
    """
    execute_query(create_relationships_query)
    execute_query("DROP POINT INDEX ON :Powiat(center)")
    execute_query("DROP INDEX ON :Powiat(id)")
    execute_query("DROP INDEX ON :Voivodship(id)")


def create_relationship_4():
    """
    Voivodship which are within country boundaries
    """
    execute_query("CREATE POINT INDEX ON :Voivodship(center)")
    execute_query("CREATE INDEX ON :Voivodship(id)")
    execute_query("CREATE INDEX ON :Country(id)")
    query = """
    MATCH (country:Country) 
    WITH  country.lower_left_corner as llc, country.upper_right_corner as urc, country
    MATCH (voivodship:Voivodship) 
    WHERE point.withinbbox(voivodship.center, llc, urc)
    RETURN voivodship.id AS voivodship_id, voivodship.center.x AS voivodship_x, voivodship.center.y AS voivodship_y, country.id AS country_id, country.wkt AS country_wkt
    """
    headers = ["voivodship_id", "country_id"]
    output_file = "/data/voivodship_country_data.csv"
    execute_query_to_csv(
        query, headers, output_file, modifier_function=is_point_within_border
    )

    create_relationships_query = f"""
    LOAD CSV FROM '{output_file}' WITH HEADER AS row
    MATCH (voivodship:Voivodship {{id: toInteger(row.voivodship_id)}}), (country:Country {{id: toInteger(row.country_id)}})
    CREATE (voivodship)-[:LOCATED_IN]->(country)
    """
    execute_query(create_relationships_query)
    execute_query("DROP POINT INDEX ON :Voivodship(center)")
    execute_query("DROP INDEX ON :Voivodship(id)")
    execute_query("DROP INDEX ON :Country(id)")


def are_adjacent(data):
    """Checks if two borders are adjacent."""
    border_1_id, border_1_wkt, border_2_id, border_2_wkt = data
    line1 = wkt.loads(border_1_wkt)
    line2 = wkt.loads(border_2_wkt)

    # A and B overlap if they have some but not all points in common,
    # have the same dimension, and the intersection of the interiors
    # of the two geometries has the same dimension as the geometries themselves.
    # That is, only polyons can overlap other polygons and only lines can overlap
    # other lines.
    # if isinstance(line1, LineString) and isinstance(line2, LineString):
    #     if line1.touches(line2) or line1.overlaps(line2):
    #         return [border_1_id, border_2_id]
    # elif isinstance(line1, MultiLineString) and isinstance(line2, LineString):
    #     for subline1 in line1.geoms:
    #         if subline1.touches(line2) or subline1.overlaps(line2):
    #             return [border_1_id, border_2_id]
    if line1.touches(line2) or line1.overlaps(line2):
        return [border_1_id, border_2_id]
    return None


def create_relationship_5():
    """
    Neighbouring (adjacent) communes
    """
    execute_query("CREATE POINT INDEX ON :Commune(center)")
    execute_query("CREATE INDEX ON :Commune(id)")
    query = """
        MATCH (c_max:Commune)
        WITH MAX(point.distance(c_max.upper_right_corner, c_max.lower_left_corner)) as max_dia
        MATCH (c1:Commune), (c2:Commune)
        WHERE id(c1) < id(c2) AND point.distance(c1.center, c2.center) <= max_dia
        RETURN c1.id AS commune1_id, c1.wkt AS commune1_wkt, 
        c2.id AS commune2_id, c2.wkt AS commune2_wkt
    """
    headers = ["commune1_id", "commune2_id"]
    output_file = "/data/adjacent_communes.csv"
    execute_query_to_csv(query, headers, output_file, modifier_function=are_adjacent)
    create_relationships_query = f"""
    LOAD CSV FROM '{output_file}' WITH HEADER AS row
    MATCH (c1:Commune {{id: toInteger(row.commune1_id)}}), (c2:Commune {{id: toInteger(row.commune2_id)}})
    CREATE (c1)-[:IS_ADJACENT]->(c2), (c2)-[:IS_ADJACENT]->(c1)
    """
    execute_query(create_relationships_query)

    execute_query("DROP POINT INDEX ON :Commune(center)")
    execute_query("DROP INDEX ON :Commune(id)")


def check_proximity(data, distance=500):
    id1, wkt1, id2, wkt2 = data

    geom1 = wkt.loads(wkt1)
    geom2 = wkt.loads(wkt2)

    actual_distance = geom1.distance(geom2)
    if actual_distance <= distance:
        return [id1, id2, actual_distance]

    return None


# id1    id2 actual_distance
def create_buildings_distance_connetions_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (startNode:Building {{id: toInteger(row.id1)}}), (endNode:Building {{id: toInteger(row.id2)}})
        CREATE (startNode)-[:CLOSE_TO {{distance: toFloat(row.actual_distance)}}]->(endNode)
    """


def create_relationship_6():
    """
    All neighbouring buildings not further than 500 meters apart; attributes: distance (meters)
    """
    query = """
        MATCH (t:Building)
        WITH MAX(t.radius) as max_r
        MATCH (t1:Building)
        WITH t1, t1.center as p, (500 + max_r + t1.radius) as max_distance
        MATCH (t2:Building)
        WHERE id(t1) <> id(t2) AND point.distance(t2.center, p) <= max_distance
        RETURN t1.id, t1.wkt, t2.id, t2.wkt
        LIMIT 10000000
        """
    headers = ["id1", "id2", "actual_distance"]

    output_directory = "/data/buildings_distance"
    # if clear_precomputed:
    #     shutil.rmtree(output_directory)
    #     if os.path.exists(output_directory):
    #         os.rmdir(output_directory)

    if not os.path.exists(output_directory):
        execute_query("CREATE INDEX ON :Building")
        execute_query("CREATE POINT INDEX ON :Building(center)")

        execute_query_to_csv_parallelized(
            query,
            headers,
            output_directory,
            modifier_function=check_proximity,
            chunk_size=100_000,
        )
        execute_query("DROP INDEX ON :Building")
        execute_query("DROP POINT INDEX ON :Building(center)")

    execute_query("CREATE INDEX ON :Building(id)")
    buildings_distance_connetions_queries = [
        create_buildings_distance_connetions_query(
            os.path.join(output_directory, file.name)
        )
        for file in Path(output_directory).glob("*.csv")
    ]
    execute_with_pool(
        execute_query, buildings_distance_connetions_queries, max_processes=10
    )
    execute_query("DROP INDEX ON :Building(id)")
    execute_query("FREE MEMORY")


def create_relationship_7():
    """
    All neighbouring trees not further than 50 meters apart; attributes: distance (meters)
    """
    execute_query("CREATE INDEX ON :Tree")
    execute_query("CREATE POINT INDEX ON :Tree(geometry)")
    execute_query(
        f"""
                    MATCH (t1:Tree)
                    WITH t1, t1.geometry as p
                    MATCH (t2:Tree)
                    WHERE id(t1) <> id(t2) AND point.distance(t2.geometry, p) <= 50
                    CREATE (t1)-[r:CLOSE_TO {{distance: point.distance(p, t2.geometry)}}]->(t2)
                  """
    )
    execute_query("DROP POINT INDEX ON :Tree(geometry)")
    execute_query("DROP INDEX ON :Tree")


def check_trees_for_distance(data, distance=20):
    road_id, road_wkt, tree_ids, tree_xs, tree_ys = data
    trees = [(tree_id, Point(float(tree_x), float(tree_y))) for tree_id, tree_x, tree_y in zip(tree_ids, tree_xs, tree_ys) ]
    road_geom = wkt.loads(road_wkt)

    prepare(road_geom)
    return_values = [(road_id, tree_id) for tree_id, tree in trees if road_geom.dwithin(tree, distance)]
    
    return None if len(return_values) == 0 else return_values

# id1    id2 actual_distance
def create_road_tree_connetions_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (road:Road {{id: toInteger(row.road_id)}}), (tree:Tree {{id: toInteger(row.tree_id)}})
        CREATE (tree)-[:NEAR]->(road)
    """

def create_relationship_8():
    """
    Trees which are not further than 20 meters from a road
    """

    query = """
    MATCH (road:Road)
    WITH road, point({x: road.lower_left_corner.x - 20, y: road.lower_left_corner.y - 20}) as llc, point({x: road.upper_right_corner.x + 20, y: road.upper_right_corner.y + 20}) as urc
    MATCH (tree:Tree)
    WHERE point.withinbbox(tree.geometry, llc, urc)
    RETURN road.id, road.wkt,  COLLECT(tree.id) as tree_ids, COLLECT(tree.geometry.x) as tree_xs, COLLECT(tree.geometry.y) as tree_ys
    """
    
    headers = ["road_id", "tree_id"]

    output_directory = "/data/trees_roads"
    clear_precomputed = True
    if clear_precomputed:
        shutil.rmtree(output_directory)
        if os.path.exists(output_directory):
            os.rmdir(output_directory)

    execute_query("CREATE INDEX ON :Tree(id)")
    
    if not os.path.exists(output_directory):
        execute_query("CREATE POINT INDEX ON :Tree(geometry)")
        execute_query("CREATE INDEX ON :Road")

        execute_query_to_csv_parallelized(
            query,
            headers,
            output_directory,
            modifier_function=check_trees_for_distance,
            expand_output_list=True,
            chunk_size=5000,
        )
        
        execute_query("DROP POINT INDEX ON :Tree(geometry)")
        execute_query("DROP INDEX ON :Road")

    execute_query("CREATE INDEX ON :Road(id)")

    road_tree_connetions_queries = [
        create_road_tree_connetions_query(
            os.path.join(output_directory, file.name)
        )
        for file in Path(output_directory).glob("*.csv")
    ]
    
    execute_with_pool(
        execute_query, road_tree_connetions_queries, max_processes=10
    )
    execute_query("DROP INDEX ON :Tree(id)")
    execute_query("DROP INDEX ON :Road(id)")
    execute_query("FREE MEMORY")
    


def create_relationship_9():
    """
    Roads which are connected through nodes;
    attributes: connecting node identifier,
    which part of one road is connected to the other road (start, mid, end)
    """
    pass


def create_relationship_10():
    """
    Railways which cross roads; attributes: angle
    """
    pass


RELATIONSHIP_CREATORS = {
    "1": create_relationship_1,
    "2": create_relationship_2,
    "3": create_relationship_3,
    "4": create_relationship_4,
    "5": create_relationship_5,
    "6": create_relationship_6,
    "7": create_relationship_7,
    "8": create_relationship_8,
    "9": create_relationship_9,
    "10": create_relationship_10,
}
