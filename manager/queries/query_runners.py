import geopandas as gpd
import numpy as np
import pandas as pd
import gc
from shapely import wkt, prepare
from shapely.geometry import Point, Polygon, LineString, MultiLineString
import json

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
    get_query_results_list,
)

def save_object_to_json(object, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(object, f, indent=4, ensure_ascii=False)
    print(f"Data saved to {filepath}")
        
def run_query_1():
    """
        Number of cities within each commune
    """
    query = """
        MATCH (c:Commune)
        MATCH (c2:City)-[:LOCATED_IN]->(c)
        RETURN c.id AS commune_id, c.name AS commune_name, COUNT(c2) AS number_of_cities
    """
    def city_within_communes_transformation_function(record):
        return {
            "id": record["commune_id"],
            "name": record["commune_name"],
            "number_of_cities_within": record["number_of_cities"]
        }

    output_filename = "query1.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Commune")
    data = get_query_results_list(query, city_within_communes_transformation_function)       
    save_object_to_json(data, output_filepath)

def run_query_2():
    """
        Adjacent powiats
    """
    query = """
        MATCH (p1:Powiat)
        MATCH (c1:Commune)-[:LOCATED_IN]->(p1)
        MATCH (c1)-[:IS_ADJACENT]->(c2)
        MATCH (p2:Powiat)<-[:LOCATED_IN]-(c2)
        WHERE p1 <> p2
        RETURN p1.id as id, p1.name as name, collect(DISTINCT [p2.id, p2.name]) as neighbours
    """
    def adjacent_powiats_transformation_function(record):
        return {
            "id": record["id"],
            "name": record["name"],
            "adjacent_powiats": [{'id': el[0], 'name': el[1]} for el in record["neighbours"]]
        }

    output_filename = "query2.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Commune")
    execute_query("CREATE INDEX ON :Powiat")
    data = get_query_results_list(query, adjacent_powiats_transformation_function)       
    save_object_to_json(data, output_filepath)

def run_query_3():
    """
        Adjacent voivodships
    """
    query = """
        MATCH (v1:Voivodship)
        MATCH (p1:Powiat)-[:LOCATED_IN]->(v1)
        MATCH (c1:Commune)-[:LOCATED_IN]->(p1)
        MATCH (c1)-[:IS_ADJACENT]->(c2)
        MATCH (c2)-[:LOCATED_IN]->(p2:Powiat)
        WHERE p1 <> p2
        MATCH (p2)-[:LOCATED_IN]->(v2:Voivodship)
        WHERE v1 <> v2
        RETURN v1.id as id, v1.name as name, collect(DISTINCT [v2.id, v2.name]) as neighbours
    """
    def adjacent_voivodships_transformation_function(record):
        return {
            "id": record["id"],
            "name": record["name"],
            "adjacent_voivodships": [{'id': el[0], 'name': el[1]} for el in record["neighbours"]]
        }

    output_filename = "query3.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Commune")
    execute_query("CREATE INDEX ON :Powiat")
    execute_query("CREATE INDEX ON :Voivodship")
    data = get_query_results_list(query, adjacent_voivodships_transformation_function)       
    save_object_to_json(data, output_filepath)

def run_query_4():
    """
        Clusters of buildings; parameters: max distance, building type, min count
    """
    pass

def run_query_5():
    """
        Road/railway crossings; parameters: min angle, max angle
    """
    pass

def run_query_6():
    """
        Roads which run parallel to railways; parameters: to be agreed
    """
    pass

def run_query_7():
    """
        Clusters of trees; parameters: max distance, min count; returned as concave hulls
    """
    pass

def run_query_8():
    """
        Shortest path between two indicated roads; parameters: start and end road ids
    """
    pass

def run_query_9():
    """
        Quasi-roundabouts: find cycles consisting of one-way streets connected end-to-end; parameters: max length
    """
    pass

def run_query_10():
    """
        Roads with trees near them; parameters: min count, max distance
    """
    pass

QUERY_RUNNERS = {
    "1": run_query_1,
    "2": run_query_2,
    "3": run_query_3,
    "4": run_query_4,
    "5": run_query_5,
    "6": run_query_6,
    "7": run_query_7,
    "8": run_query_8,
    "9": run_query_9,
    "10": run_query_10,
}
