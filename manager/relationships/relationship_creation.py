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

from settings import get_clear_preprocessed_value
from utils.parallelization import execute_with_pool
from database.communication import (
    execute_query,
    execute_query_to_csv,
    execute_query_to_csv_parallelized,
)

def clear_preprocessed_check(output_directory):
    if get_clear_preprocessed_value() and os.path.exists(output_directory):
        shutil.rmtree(output_directory)
        if os.path.exists(output_directory):
            os.rmdir(output_directory)
    

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
    query = """
    MATCH (commune:Commune) 
    WITH  commune.lower_left_corner as llc, commune.upper_right_corner as urc, commune
    MATCH (city:City) 
    WHERE point.withinbbox(city.center, llc, urc)
    RETURN city.id AS city_id, city.center.x AS city_x, city.center.y AS city_y, commune.id AS commune_id, commune.wkt AS commune_wkt
    """
    headers = ["city_id", "commune_id"]
    output_directory = "/data/city_commune_data"
    clear_preprocessed_check(output_directory)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
        execute_query("CREATE POINT INDEX ON :City(center)")
        execute_query("CREATE INDEX ON :Commune")
        execute_query_to_csv_parallelized(
            query, headers, output_directory, modifier_function=is_point_within_border, chunk_size=2000
        )

        execute_query("DROP POINT INDEX ON :City(center)")
        execute_query("DROP INDEX ON :Commune")
        

    create_relationships_query = lambda path: f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (city:City {{id: toInteger(row.city_id)}}), (commune:Commune {{id: toInteger(row.commune_id)}})
        CREATE (city)-[:LOCATED_IN]->(commune)
        """
    
    city_commune_connetions_queries = [
        create_relationships_query(
            os.path.join(output_directory, file.name)
        )
        for file in Path(output_directory).glob("*.csv")
    ]

    execute_query("CREATE INDEX ON :City(id)")
    execute_query("CREATE INDEX ON :Commune(id)")
    
    execute_with_pool(
        execute_query, city_commune_connetions_queries, max_processes=10
    )
    
    execute_query("DROP INDEX ON :City(id)")
    execute_query("DROP INDEX ON :Commune(id)")
    execute_query("FREE MEMORY")


def create_relationship_2():
    """
    Communes which are within powiat boundaries
    """
    query = """
    MATCH (powiat:Powiat) 
    WITH  powiat.lower_left_corner as llc, powiat.upper_right_corner as urc, powiat
    MATCH (commune:Commune) 
    WHERE point.withinbbox(commune.center, llc, urc)
    RETURN commune.id AS commune_id, commune.center.x AS commune_x, commune.center.y AS commune_y, powiat.id AS powiat_id, powiat.wkt AS powiat_wkt
    """
    headers = ["commune_id", "powiat_id"]
    output_directory = "/data/commune_powiat_data"
    clear_preprocessed_check(output_directory)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
        execute_query("CREATE POINT INDEX ON :Commune(center)")
        execute_query("CREATE INDEX ON :Powiat")

        execute_query_to_csv_parallelized(
            query, headers, output_directory, modifier_function=is_point_within_border, chunk_size=1000
        )

        execute_query("DROP POINT INDEX ON :Commune(center)")
        execute_query("DROP INDEX ON :Powiat")

        
    create_relationships_query = lambda path: f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (commune:Commune {{id: toInteger(row.commune_id)}}), (powiat:Powiat {{id: toInteger(row.powiat_id)}})
        CREATE (commune)-[:LOCATED_IN]->(powiat)
        """
        
    commune_powiat_connetions_queries = [
        create_relationships_query(
            os.path.join(output_directory, file.name)
        )
        for file in Path(output_directory).glob("*.csv")
    ]
            
    execute_query("CREATE INDEX ON :Powiat(id)")
    execute_query("CREATE INDEX ON :Commune(id)")
    
    execute_with_pool(
        execute_query, commune_powiat_connetions_queries, max_processes=10
    )

    execute_query("DROP INDEX ON :Powiat(id)")
    execute_query("DROP INDEX ON :Commune(id)")
    execute_query("FREE MEMORY")


def create_relationship_3():
    """
    Powiats which are within voivodship boundaries
    """

    query = """
    MATCH (voivodship:Voivodship) 
    WITH  voivodship.lower_left_corner as llc, voivodship.upper_right_corner as urc, voivodship
    MATCH (powiat:Powiat) 
    WHERE point.withinbbox(powiat.center, llc, urc)
    RETURN powiat.id AS powiat_id, powiat.center.x AS powiat_x, powiat.center.y AS powiat_y, voivodship.id AS voivodship_id, voivodship.wkt AS voivodship_wkt
    """
    headers = ["powiat_id", "voivodship_id"]
    output_directory = "/data/powiat_voivodship_data"
    output_file = os.path.join(output_directory, 'powiat_voivodship_data.csv')
    clear_preprocessed_check(output_directory)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
        execute_query("CREATE POINT INDEX ON :Powiat(center)")
        execute_query("CREATE INDEX ON :Voivodship")
        
        execute_query_to_csv(
            query, headers, output_file, modifier_function=is_point_within_border
        )
        
        execute_query("DROP POINT INDEX ON :Powiat(center)")
        execute_query("DROP INDEX ON :Voivodship")
        
    create_relationships_query = f"""
        LOAD CSV FROM '{output_file}' WITH HEADER AS row
        MATCH (powiat:Powiat {{id: toInteger(row.powiat_id)}}), (voivodship:Voivodship {{id: toInteger(row.voivodship_id)}})
        CREATE (powiat)-[:LOCATED_IN]->(voivodship)
        """
        
    execute_query("CREATE INDEX ON :Powiat(id)")
    execute_query("CREATE INDEX ON :Voivodship(id)")

    execute_query(create_relationships_query)
    
    execute_query("DROP INDEX ON :Powiat(id)")
    execute_query("DROP INDEX ON :Voivodship(id)")
    execute_query("FREE MEMORY")


def create_relationship_4():
    """
    Voivodship which are within country boundaries
    """

    query = """
    MATCH (country:Country) 
    WITH  country.lower_left_corner as llc, country.upper_right_corner as urc, country
    MATCH (voivodship:Voivodship) 
    WHERE point.withinbbox(voivodship.center, llc, urc)
    RETURN voivodship.id AS voivodship_id, voivodship.center.x AS voivodship_x, voivodship.center.y AS voivodship_y, country.id AS country_id, country.wkt AS country_wkt
    """
    headers = ["voivodship_id", "country_id"]
    output_directory = "/data/voivodship_country_data"
    output_file = os.path.join(output_directory, 'voivodship_country_data.csv')
    clear_preprocessed_check(output_directory)
            
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
        execute_query("CREATE POINT INDEX ON :Voivodship(center)")
        execute_query("CREATE INDEX ON :Country")
        
        execute_query_to_csv(
            query, headers, output_file, modifier_function=is_point_within_border
        )
        
        execute_query("DROP INDEX ON :Country")
        execute_query("DROP POINT INDEX ON :Voivodship(center)")

    create_relationships_query = f"""
        LOAD CSV FROM '{output_file}' WITH HEADER AS row
        MATCH (voivodship:Voivodship {{id: toInteger(row.voivodship_id)}}), (country:Country {{id: toInteger(row.country_id)}})
        CREATE (voivodship)-[:LOCATED_IN]->(country)
        """
        
    execute_query("CREATE INDEX ON :Voivodship(id)")
    execute_query("CREATE INDEX ON :Country(id)")
    
    execute_query(create_relationships_query)
    
    execute_query("DROP INDEX ON :Voivodship(id)")
    execute_query("DROP INDEX ON :Country(id)")
    execute_query("FREE MEMORY")


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

    if line1.touches(line2) or line1.overlaps(line2):
        return [border_1_id, border_2_id]
    return None


def create_relationship_5():
    """
    Neighbouring (adjacent) communes
    """

    query = """
        MATCH (c_max:Commune)
        WITH MAX(point.distance(c_max.upper_right_corner, c_max.lower_left_corner)) as max_dia
        MATCH (c1:Commune), (c2:Commune)
        WHERE id(c1) < id(c2) AND point.distance(c1.center, c2.center) <= max_dia
        RETURN c1.id AS commune1_id, c1.wkt AS commune1_wkt, 
        c2.id AS commune2_id, c2.wkt AS commune2_wkt
    """
    headers = ["commune1_id", "commune2_id"]
    output_directory = "/data/adjacent_communes"
    clear_preprocessed_check(output_directory)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
        execute_query("CREATE POINT INDEX ON :Commune(center)")
        execute_query("CREATE INDEX ON :Commune")
        
        execute_query_to_csv_parallelized(
            query, headers, output_directory, modifier_function=are_adjacent, chunk_size=1000
        )

        execute_query("DROP POINT INDEX ON :Commune(center)")
        execute_query("DROP INDEX ON :Commune")
        
    create_relationships_query = lambda path: f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (c1:Commune {{id: toInteger(row.commune1_id)}}), (c2:Commune {{id: toInteger(row.commune2_id)}})
        CREATE (c1)-[:IS_ADJACENT]->(c2), (c2)-[:IS_ADJACENT]->(c1)
        """
        
    adjcent_communes_connetions_queries = [
        create_relationships_query(
            os.path.join(output_directory, file.name)
        )
        for file in Path(output_directory).glob("*.csv")
    ]
    execute_query("CREATE INDEX ON :Commune(id)")

    execute_with_pool(
        execute_query, adjcent_communes_connetions_queries, max_processes=10
    )
    
    execute_query("DROP INDEX ON :Commune(id)")
    execute_query("FREE MEMORY")


def check_proximity(data, distance=500):
    id1, wkt1, id2, wkt2 = data

    geom1 = wkt.loads(wkt1)
    geom2 = wkt.loads(wkt2)

    actual_distance = geom1.distance(geom2)
    if actual_distance <= distance:
        return [id1, id2, actual_distance]

    return None

def check_proximity_multiple(data, distance=500):
    id1, wkt1, ids, wkts = data

    geom1 = wkt.loads(wkt1)
    prepare(geom1)
    
    return_values = []
    for id2, wkt2 in zip(ids, wkts):
        geom2 = wkt.loads(wkt2)
        actual_distance = geom1.distance(geom2)
        if actual_distance <= distance:
            return_values.append([id1, id2, actual_distance]) 

    return None if len(return_values) == 0 else return_values

# id1    id2 actual_distance
def create_buildings_distance_connetions_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (startNode:Building {{id: toInteger(row.id1)}}), (endNode:Building {{id: toInteger(row.id2)}})
        CREATE (startNode)-[:CLOSE_TO {{distance: toFloat(row.actual_distance)}}]->(endNode), (endNode)-[:CLOSE_TO {{distance: toFloat(row.actual_distance)}}]->(startNode)
    """


def create_relationship_6():
    """
    All neighbouring buildings not further than 500 meters apart; attributes: distance (meters)
    """
    query = """
        MATCH (wieliczka: Powiat{name:"powiat wielicki"})
        WITH wieliczka.lower_left_corner as llc, wieliczka.upper_right_corner as urc
        
        MATCH (t:Building)
        WHERE point.withinbbox(t.center, llc, urc)
        WITH MAX(t.radius) as max_r, llc, urc
        
        MATCH (t1:Building)
        WHERE point.withinbbox(t1.center, llc, urc)
        WITH t1, t1.center as p, (500 + max_r + t1.radius) as max_distance, max_r
        
        MATCH (t2:Building)
        WHERE id(t1) < id(t2) AND point.distance(t2.center, p) <= max_distance
        RETURN t1.id, t1.wkt, t2.id, t2.wkt
        """
    headers = ["id1", "id2", "actual_distance"]

    output_directory = "/data/buildings_distance"
    clear_preprocessed_check(output_directory)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
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
                    WHERE id(t1) < id(t2) AND point.distance(t2.geometry, p) <= 50
                    CREATE (t1)-[:CLOSE_TO {{distance: point.distance(p, t2.geometry)}}]->(t2), (t2)-[:CLOSE_TO {{distance: point.distance(p, t2.geometry)}}]->(t1)
                  """
    )
    execute_query("DROP POINT INDEX ON :Tree(geometry)")
    execute_query("DROP INDEX ON :Tree")
    execute_query("FREE MEMORY")


def check_trees_for_distance(data, distance=20):
    road_id, road_wkt, tree_ids, tree_xs, tree_ys = data
    trees = [(tree_id, Point(float(tree_x), float(tree_y))) for tree_id, tree_x, tree_y in zip(tree_ids, tree_xs, tree_ys) ]
    road_geom = wkt.loads(road_wkt)

    prepare(road_geom)
    return_values = [(road_id, tree_id, road_geom.distance(tree)) for tree_id, tree in trees if road_geom.dwithin(tree, distance)]
    
    return None if len(return_values) == 0 else return_values

# id1    id2 actual_distance
def create_road_tree_connetions_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (road:Road {{id: toInteger(row.road_id)}}), (tree:Tree {{id: toInteger(row.tree_id)}})
        CREATE (tree)-[:CLOSE_TO {{distance: toFloat(row.distance)}}]->(road)
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
    
    headers = ["road_id", "tree_id", "distance"]

    output_directory = "/data/trees_roads"
    clear_preprocessed_check(output_directory)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
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

    execute_query("CREATE INDEX ON :Tree(id)")
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
    query = """
    MATCH (rn:RoadNode)
    MATCH (rn)-[:BELONGS_TO]->(r:Road)
    WITH rn, COLLECT(r) as roads, extract(road IN COLLECT(r)| CASE WHEN road.start_node_id = rn.id THEN "start" WHEN road.end_node_id = rn.id THEN "end" ELSE "mid" END) as parts
    WHERE SIZE(roads) > 1
    FOREACH (index1 IN RANGE(0, SIZE(roads) - 2) |
        FOREACH (index2 IN RANGE(index1 + 1, SIZE(roads) - 1) |
            FOREACH (firstRoad IN [roads[index1]] |
                FOREACH (secondRoad IN [roads[index2]] | 
                    CREATE (firstRoad)-[:ROAD_CONNECTED_TO {rn_id: rn.id, part: parts[index1]}]->(secondRoad)
                    CREATE (secondRoad)-[:ROAD_CONNECTED_TO {rn_id: rn.id, part: parts[index2]}]->(firstRoad)
                )
            )
        )
    )
    """
    
    execute_query("CREATE INDEX ON :Road(id)")
    execute_query("CREATE INDEX ON :Road")
    execute_query("CREATE INDEX ON :RoadNode")
    
    execute_query(query)

    execute_query("DROP INDEX ON :Road(id)")
    execute_query("DROP INDEX ON :Road")
    execute_query("DROP INDEX ON :RoadNode")
    execute_query("FREE MEMORY")
    
    
def calculate_angle(segment1, segment2):

    x1, y1, x2, y2 = *segment1.coords[0], *segment1.coords[1]
    x3, y3, x4, y4 = *segment2.coords[0], *segment2.coords[1]

    vector1 = (x2 - x1, y2 - y1)
    vector2 = (x4 - x3, y4 - y3)

    dot_product = vector1[0] * vector2[0] + vector1[1] * vector2[1]
    magnitude1 = np.sqrt(vector1[0]**2 + vector1[1]**2)
    magnitude2 = np.sqrt(vector2[0]**2 + vector2[1]**2)

    cos_theta = dot_product / (magnitude1 * magnitude2)
    cos_theta = min(1, max(-1, cos_theta)) 

    angle = np.acos(cos_theta) * (180 / np.pi)
    return angle
    
def check_railroad_road_intersection(data):
    railroad_id, railroad_wkt, road_ids, road_wkts = data
    roads = [(road_id, wkt.loads(road_wkt)) for road_id, road_wkt in zip(road_ids, road_wkts) ]
    railroad_geom = wkt.loads(railroad_wkt)
    prepare(railroad_geom)
    
    return_values = []
    for road_id, road in roads:
        if railroad_geom.crosses(road):
            for i in range(len(railroad_geom.coords) - 1):
                railroad_segment = LineString([railroad_geom.coords[i], railroad_geom.coords[i + 1]])
                prepare(railroad_segment)

                for j in range(len(road.coords) - 1):
                    road_segment = LineString([road.coords[j], road.coords[j + 1]])

                    if railroad_segment.crosses(road_segment):
                        angle = calculate_angle(railroad_segment, road_segment)
                        return_values.append((railroad_id, road_id, angle))

    
    return None if len(return_values) == 0 else return_values    

# ["railway_id", "road_id", "angle"]
def create_road_railway_crossing_query(path):
    return f"""
        LOAD CSV FROM '{path}' WITH HEADER AS row
        MATCH (railway:Railway {{id: toInteger(row.railway_id)}}), (road:Road {{id: toInteger(row.road_id)}})
        CREATE (railway)-[:CROSSES {{angle: toFloat(row.angle)}}]->(road)
    """

def create_relationship_10():
    """
    Railways which cross roads; attributes: angle
    """
    
    query="""
    MATCH (ra:Railway)
    WITH point.distance(ra.upper_right_corner, ra.lower_left_corner) as max_distance, ra, ra.upper_right_corner as p
    MATCH (r:Road)
    WHERE point.distance(r.upper_right_corner, p) <= max_distance AND
        ra.upper_right_corner.x >= r.lower_left_corner.x AND 
        r.upper_right_corner.x >= ra.lower_left_corner.x AND 
        ra.upper_right_corner.y >= r.lower_left_corner.y AND 
        r.upper_right_corner.y >= ra.lower_left_corner.y
    RETURN ra.id, ra.wkt, COLLECT(r.id), COLLECT(r.wkt)
    """
    
    headers = ["railway_id", "road_id", "angle"]

    output_directory = "/data/railway_road_intersections"
    clear_preprocessed_check(output_directory)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)
        execute_query("CREATE POINT INDEX ON :Road(upper_right_corner)")
        execute_query("CREATE INDEX ON :Railway")

        execute_query_to_csv_parallelized(
            query,
            headers,
            output_directory,
            modifier_function=check_railroad_road_intersection,
            expand_output_list=True,
            chunk_size=1000,
        )
        
        execute_query("DROP POINT INDEX ON :Road(upper_right_corner)")
        execute_query("DROP INDEX ON :Railway")

    execute_query("CREATE INDEX ON :Road(id)")
    execute_query("CREATE INDEX ON :Railway(id)")

    road_railway_crossing_queries = [
        create_road_railway_crossing_query(
            os.path.join(output_directory, file.name)
        )
        for file in Path(output_directory).glob("*.csv")
    ]
    
    execute_with_pool(
        execute_query, road_railway_crossing_queries, max_processes=10
    )
    execute_query("DROP INDEX ON :Railway(id)")
    execute_query("DROP INDEX ON :Road(id)")
    execute_query("FREE MEMORY")


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
