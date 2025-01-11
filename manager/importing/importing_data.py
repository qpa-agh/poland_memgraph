from pathlib import Path
import shutil
import os
import pandas as pd
import gc

from utils.file_management import prepare_files, find_file, prepare_paths, split_large_csv
from utils.parallelization import execute_with_pool
from database.communication import execute_query
from settings import get_clear_preprocessed_value

from importing.data_specific.buildings import (
    preprocess_buildings_df,
    create_buildings_input_query,
)
from importing.data_specific.cities import (
    preprocess_cities_df,
    create_cities_input_query,
)
from importing.data_specific.communes import (
    preprocess_communes_df,
    create_communes_input_query,
)
from importing.data_specific.countries import (
    preprocess_countries_df,
    create_countries_input_query,
)
from importing.data_specific.powiats import (
    preprocess_powiats_df,
    create_powiats_input_query,
)
from importing.data_specific.trees import (
    preprocess_trees_df, 
    create_trees_input_query,
)
from importing.data_specific.voivodships import (
    preprocess_voivodships_df,
    create_voivodships_input_query,
)
from importing.data_specific.roads import (
    preprocess_roads_df,
    create_roads_input_query,
    preprocess_road_node_df,
    create_road_node_input_query,
    preprocess_road_node_connections,
    create_road_node_connection_input_query,
    create_road_node_road_connection_query,
)
from importing.data_specific.railways import (
    preprocess_railways_df,
    create_railways_input_query,
)


def default_loading(name, preprocess_function, input_query_creation_function, max_rows=1_000_000):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_function, max_rows=max_rows, clear_output=get_clear_preprocessed_value())
    queries = [
        input_query_creation_function(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries, max_processes=10)
    execute_query('FREE MEMORY')

def load_buildings(name="buildings"):
    default_loading(name, preprocess_buildings_df, create_buildings_input_query, max_rows=500_000)

def load_cities(name="cities"):
    default_loading(name, preprocess_cities_df, create_cities_input_query)

def load_communes(name="communes"):
    default_loading(name, preprocess_communes_df, create_communes_input_query)

def load_countries(name="countries"):
    default_loading(name, preprocess_countries_df, create_countries_input_query)

def load_powiats(name="powiats"):
    default_loading(name, preprocess_powiats_df, create_powiats_input_query)

def load_railways(name="railways"):
    default_loading(name, preprocess_railways_df, create_railways_input_query)
    
def load_trees(name="trees"):
    default_loading(name, preprocess_trees_df, create_trees_input_query)

def load_voivodships(name="voivodships"):
    default_loading(name, preprocess_voivodships_df, create_voivodships_input_query)
    
    
def preprocess_and_save_road_components(name):
    filename = find_file(name)
    source_filepath = os.path.join("/data", filename)
    roads_directory = '/data/roads'
    roadnodes_directory = '/data/roadnodes'
    roadnodes_roads_connection_directory = '/data/roadnodes_roads'
    roadnodes_roadnodes_connection_directory = '/data/roadnodes_roadnodes'
    
    dirs = [roads_directory, roadnodes_directory, roadnodes_roads_connection_directory, roadnodes_roadnodes_connection_directory]
    any_missing = any(not os.path.exists(directory) for directory in dirs)
    for directory in dirs:
        if os.path.exists(directory) and (get_clear_preprocessed_value() or any_missing):
            shutil.rmtree(directory)
            if os.path.exists(directory):
                os.rmdir(directory)

    if not os.path.exists(roads_directory):
        roads_df = pd.read_csv(source_filepath, low_memory=False)
        roads_df, df = preprocess_roads_df(roads_df)
        print("roads preprocessed")
        
        roads_df_file_path = os.path.join('/data', f"roads.csv")
        roads_df.to_csv(roads_df_file_path, index=False)
        del roads_df
        gc.collect()


        # ================= ROADNODES =====================
        road_node_df_file_path = os.path.join('/data', f"road_nodes.csv")
        road_node_df = preprocess_road_node_df(df)
        road_node_df.drop_duplicates(subset="id").to_csv(road_node_df_file_path, index=False)
        
        # ================= ROADNODE ROAD CONNECTION =====================
        road_node_road_connections_df_file_path = os.path.join('/data', f"road_node_road_connections.csv")
        road_node_df[['id', 'road_id']].drop_duplicates(subset=['id', 'road_id']).to_csv(road_node_road_connections_df_file_path, index=False)
        del road_node_df
        gc.collect()
        print("road nodes preprocessed")
        
        
        # ================= ROADNODES CONNECTIONS =====================
        road_node_connetions_df_file_path = os.path.join('/data', f"road_node_connections.csv")
        road_node_connetions_df = preprocess_road_node_connections(df)
        road_node_connetions_df.to_csv(road_node_connetions_df_file_path, index=False)
        del road_node_connetions_df
        del df
        gc.collect()
        print("road node connections  preprocessed ")
        
        split_large_csv(roads_df_file_path, roads_directory, max_rows=400_000)
        split_large_csv(road_node_df_file_path, roadnodes_directory)
        split_large_csv(road_node_road_connections_df_file_path, roadnodes_roads_connection_directory)
        split_large_csv(road_node_connetions_df_file_path, roadnodes_roadnodes_connection_directory)
        
        os.remove(roads_df_file_path)
        os.remove(road_node_df_file_path)
        os.remove(road_node_road_connections_df_file_path)
        os.remove(road_node_connetions_df_file_path)
        
    return dirs

def load_roads(name="roads"):
    dirs = preprocess_and_save_road_components(name)
    roads_directory, roadnodes_directory, roadnodes_roads_connection_directory, roadnodes_roadnodes_connection_directory = dirs
    # ROAD creation
    road_queries = [
        create_roads_input_query(os.path.join(roads_directory, file.name))
        for file in Path(roads_directory).glob("*.csv")
    ]
    execute_with_pool(execute_query, road_queries)
    execute_query('CREATE INDEX ON :Road(id)')
    execute_query('FREE MEMORY')

    # ROAD NODE creation
    road_nodes_queries = [
        create_road_node_input_query(os.path.join(roadnodes_directory, file.name))
        for file in Path(roadnodes_directory).glob("*.csv")
    ]
    execute_with_pool(execute_query, road_nodes_queries, max_processes=20)
    execute_query('CREATE INDEX ON :RoadNode(id)')
    execute_query('FREE MEMORY')
    
    # ROAD NODE ROAD relationship creation
    road_node_road_connections_queries = [
        create_road_node_road_connection_query(os.path.join(roadnodes_roads_connection_directory, file.name))
        for file in Path(roadnodes_roads_connection_directory).glob("*.csv")
    ]
    execute_with_pool(execute_query, road_node_road_connections_queries, max_processes=20)
    
    # ROAD NODE CONNECTION relationship creation
    road_nodes_connetions_queries = [
        create_road_node_connection_input_query(os.path.join(roadnodes_roadnodes_connection_directory, file.name))
        for file in Path(roadnodes_roadnodes_connection_directory).glob("*.csv")
    ]
    execute_with_pool(execute_query, road_nodes_connetions_queries, max_processes=20)
    
    execute_query('DROP INDEX ON :Road(id)')
    execute_query('DROP INDEX ON :RoadNode(id)')
    execute_query('FREE MEMORY')
    

DATA_LOADERS = {
    "roads": load_roads,
    "buildings": load_buildings,
    "cities": load_cities,
    "communes": load_communes,
    "countries": load_countries,
    "powiats": load_powiats,
    "railways": load_railways,
    "trees": load_trees,
    "voivodships": load_voivodships,
}
