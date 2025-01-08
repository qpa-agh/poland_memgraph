import geopandas as gpd
import numpy as np
import pandas as pd
import gc
from shapely import wkt
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
    pass


def create_relationship_5():
    """
    Neighbouring (adjacent) communes
    """
    pass


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


def create_relationship_8():
    """
    Trees which are not further than 20 meters from a road
    """
    pass


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
