from pathlib import Path
import shutil
import os
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt

from importing.utils import prepare_files, execute_with_pool
from database.communication import execute_query

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
from importing.data_specific.trees import preprocess_trees_df, create_trees_input_query
from importing.data_specific.voivodships import (
    preprocess_voivodships_df,
    create_voivodships_input_query,
)
from importing.data_specific.roads import (
    preprocess_roads_df,
    create_roads_input_query,
    preprocess_road_node_df,
    create_road_node_input_query,
)
from importing.data_specific.railways import (
    preprocess_railways_df,
    create_railways_input_query,
)


def load_buildings(name="buildings"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_buildings_df)
    queries = [
        create_buildings_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


def load_cities(name="cities"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_cities_df)
    queries = [
        create_cities_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


def load_communes(name="communes"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_communes_df)
    queries = [
        create_communes_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


def load_countries(name="countries"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_countries_df)
    queries = [
        create_countries_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


def load_powiats(name="powiats"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_powiats_df)
    queries = [
        create_powiats_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


def load_railways(name="railways"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_railways_df)
    queries = [
        create_railways_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


def load_roads(name="roads"):
    try:
        filename = next(
            file_name
            for file_name in os.listdir("/data")
            if name in file_name and file_name.lower().endswith(".csv")
        )
    except StopIteration as e:
        print(f"No such file as {name}", flush=True)
        raise e
    source_filepath = os.path.join("/data", filename)
    output_dir = os.path.join("/data", name)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    if os.path.exists(output_dir):
        os.rmdir(output_dir)

    os.makedirs(output_dir, exist_ok=True)

    roads_df = pd.read_csv(source_filepath, low_memory=False)
    roads_df, df = preprocess_roads_df(roads_df)
    print("roads preprocessed")
    roads_df_file_path = os.path.join(output_dir, f"roads_df.csv")
    roads_df.to_csv(roads_df_file_path, index=False)
    road_queries = [
        create_roads_input_query(os.path.join(output_dir, file.name))
        for file in Path(output_dir).glob("*.csv")
    ]
    del roads_df

    # ================= ROAD NODES =====================
    road_node_df = preprocess_road_node_df(df)
    road_node_df.to_csv(os.path.join("/data", "road_nodes.csv"), index=False)
    del road_node_df
    del df
    print("road nodes preprocessed")

    target_dir = prepare_files("road_nodes")
    road_nodes_queries = [
        create_road_node_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]

    execute_with_pool(execute_query, road_queries + road_nodes_queries)


def load_trees(name="trees"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_trees_df)
    queries = [
        create_trees_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


def load_voivodships(name="voivodships"):
    target_dir = prepare_files(name, dataframe_modifier=preprocess_voivodships_df)
    queries = [
        create_voivodships_input_query(os.path.join(target_dir, file.name))
        for file in Path(target_dir).glob("*.csv")
    ]
    execute_with_pool(execute_query, queries)


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
