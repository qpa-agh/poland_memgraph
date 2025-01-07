import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt
import gc

from utils.parallelization import execute_with_pool
from database.communication import execute_query




def create_relationship_1():
    """
        Cities which are within commune boundaries
    """
    pass

def create_relationship_2():
    """
        Communes which are within powiat boundaries
    """
    pass

def create_relationship_3():
    """
        Powiats which are within voivodship boundaries
    """
    pass

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

def create_relationship_6():
    """
        All neighbouring buildings not further than 500 meters apart; attributes: distance (meters)
    """
    pass

def create_relationship_7():
    """
        All neighbouring trees not further than 50 meters apart; attributes: distance (meters)
    """
    execute_query('CREATE INDEX ON :Tree')
    execute_query('CREATE POINT INDEX ON :Tree(geometry)')
    execute_query(f"""
                    MATCH (t1:Tree)
                    WITH t1, t1.geometry as p
                    MATCH (t2:Tree)
                    WHERE id(t1) <> id(t2) AND point.distance(t2.geometry, p) <= 50
                    CREATE (t1)-[r:CLOSE_TO {{distance: point.distance(p, t2.geometry)}}]->(t2)
                  """)
    execute_query('DROP POINT INDEX ON :Tree(geometry)')
    execute_query('DROP INDEX ON :Tree')

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