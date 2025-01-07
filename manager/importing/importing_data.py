from pathlib import Path
import shutil
import os
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt
import gc

from utils.file_management import prepare_files, find_file, prepare_paths
from utils.parallelization import execute_with_pool
from database.communication import execute_query

from importing.data_specific.buildings import (
    preprocess_buildings_df,
    create_buildings_input_query,
    create_building_label_index_query,
    create_building_id_index_query,
    create_building_center_point_index_query
)
from importing.data_specific.cities import (
    preprocess_cities_df,
    create_cities_input_query,
    create_city_label_index_query
)
from importing.data_specific.communes import (
    preprocess_communes_df,
    create_communes_input_query,
    create_commune_label_index_query
)
from importing.data_specific.countries import (
    preprocess_countries_df,
    create_countries_input_query,
    create_country_label_index_query
)
from importing.data_specific.powiats import (
    preprocess_powiats_df,
    create_powiats_input_query,
    create_powiat_label_index_query
)
from importing.data_specific.trees import (
    preprocess_trees_df, 
    create_trees_input_query,
    create_tree_label_index_query,
    create_tree_point_index_query
)
from importing.data_specific.voivodships import (
    preprocess_voivodships_df,
    create_voivodships_input_query,
    create_voivodship_label_index_query
)
from importing.data_specific.roads import (
    preprocess_roads_df,
    create_roads_input_query,
    preprocess_road_node_df,
    create_road_node_input_query,
    preprocess_road_node_connections,
    create_road_label_index_query,
    create_road_id_index_query,
    create_road_node_id_index_query,
    create_road_node_point_index_query,
    create_road_node_connection_input_query,
    create_road_node_road_connection_query
)
from importing.data_specific.railways import (
    preprocess_railways_df,
    create_railways_input_query,
    create_railway_label_index_query
)


def load_buildings(name="buildings"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_buildings_df)
    queries = [
        create_buildings_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries, max_processes=5)
    # execute_query(create_building_label_index_query())
    # execute_query(create_building_id_index_query())
    # execute_query(create_building_center_point_index_query())
    execute_query('FREE MEMORY')

def load_cities(name="cities"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_cities_df)
    queries = [
        create_cities_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)
    # execute_query(create_city_label_index_query())
    execute_query('FREE MEMORY')


def load_communes(name="communes"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_communes_df)
    queries = [
        create_communes_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)
    # execute_query(create_commune_label_index_query())
    execute_query('FREE MEMORY')


def load_countries(name="countries"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_countries_df)
    queries = [
        create_countries_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)
    # execute_query(create_country_label_index_query())
    execute_query('FREE MEMORY')


def load_powiats(name="powiats"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_powiats_df)
    queries = [
        create_powiats_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)
    # execute_query(create_powiat_label_index_query())
    execute_query('FREE MEMORY')


def load_railways(name="railways"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_railways_df)
    queries = [
        create_railways_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)
    # execute_query(create_railway_label_index_query())
    execute_query('FREE MEMORY')

CLEAR_ROADS = False
def load_roads(name="roads"):
    filename = find_file(name)
    source_filepath, output_dir = prepare_paths(name, filename, CLEAR_ROADS)
    if not os.path.exists(output_dir) or CLEAR_ROADS:
        os.makedirs(output_dir, exist_ok=True)

        roads_df = pd.read_csv(source_filepath, low_memory=False)
        roads_df, df = preprocess_roads_df(roads_df)
        print("roads preprocessed")
        
        roads_df_file_path = os.path.join(output_dir, f"roads_df.csv")
        roads_df.to_csv(roads_df_file_path, index=False)
        del roads_df
        gc.collect()


        # ================= ROAD NODES =====================
        road_node_df = preprocess_road_node_df(df)
        road_node_df.to_csv(os.path.join("/data", "road_nodes.csv"), index=False)
        del road_node_df
        gc.collect()

        # ================= ROAD NODES CONNECTIONS =====================
        road_node_connetions_df = preprocess_road_node_connections(df)
        road_node_connetions_df.to_csv(os.path.join("/data", "road_node_connetions.csv"), index=False)
        del road_node_connetions_df
        del df
        gc.collect()
        
    print("road nodes preprocessed")

    # ROAD creation
    road_queries = [
        create_roads_input_query(os.path.join(output_dir, file.name))
        for file in Path(output_dir).glob("*.csv")
    ]
    gc.collect()
    execute_with_pool(execute_query, road_queries)
    # execute_query(create_road_label_index_query())
    execute_query(create_road_id_index_query())
    execute_query('FREE MEMORY')

    # ROAD NODE creation
    target_dir = prepare_files("road_nodes")
    road_nodes_queries = [
        create_road_node_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]

    execute_with_pool(execute_query, road_nodes_queries, max_processes=5)
    execute_query(create_road_node_id_index_query())
    # execute_query(create_road_node_point_index_query())
    execute_query('FREE MEMORY')
    
    # execute_query('EDGE IMPORT MODE ACTIVE')
    # ROAD NODE ROAD relationship creation
    road_nodes_queries = [
        create_road_node_road_connection_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, road_nodes_queries, max_processes=5)
    
    # ROAD NODE CONNECTION creation
    target_dir = prepare_files("road_node_connetions")
    road_nodes_connetions_queries = [
        create_road_node_connection_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, road_nodes_connetions_queries, max_processes=5)
    # execute_query('EDGE IMPORT MODE INACTIVE')
    execute_query('FREE MEMORY')
    


def load_trees(name="trees"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_trees_df)
    queries = [
        create_trees_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)
    # execute_query(create_tree_label_index_query())
    # execute_query(create_tree_point_index_query())
    execute_query('FREE MEMORY')

def load_voivodships(name="voivodships"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_voivodships_df)
    queries = [
        create_voivodships_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)
    # execute_query(create_voivodship_label_index_query())
    execute_query('FREE MEMORY')


DATA_LOADERS = {
    "buildings": load_buildings,
    "cities": load_cities,
    "communes": load_communes,
    "countries": load_countries,
    "powiats": load_powiats,
    "railways": load_railways,
    "roads": load_roads,
    "trees": load_trees,
    "voivodships": load_voivodships,
}
