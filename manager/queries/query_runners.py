from shapely import wkt, prepare, convex_hull, MultiPoint, LineString
import json
import networkx as nx
import math

from utils.parallelization import parrarelize_processes
from database.communication import (
    execute_query,
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
    print(f"Running query 1")
    query = """
        MATCH (c:Commune)
        MATCH (c2:City)-[:LOCATED_IN]->(c)
        RETURN c.id AS commune_id, c.name AS commune_name, COUNT(c2) AS number_of_cities
    """

    def city_within_communes_transformation_function(record):
        return {
            "id": record["commune_id"],
            "name": record["commune_name"],
            "number_of_cities_within": record["number_of_cities"],
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
    print(f"Running query 2")
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
            "adjacent_powiats": [
                {"id": el[0], "name": el[1]} for el in record["neighbours"]
            ],
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
    print(f"Running query 3")
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
            "adjacent_voivodships": [
                {"id": el[0], "name": el[1]} for el in record["neighbours"]
            ],
        }

    output_filename = "query3.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Commune")
    execute_query("CREATE INDEX ON :Powiat")
    execute_query("CREATE INDEX ON :Voivodship")
    data = get_query_results_list(query, adjacent_voivodships_transformation_function)
    save_object_to_json(data, output_filepath)


def run_query_4(max_distance, building_type, min_count):
    """
    Clusters of buildings; parameters: max distance, building type, min count
    """
    max_distance = float(max_distance)
    min_count = int(min_count)
    print(
        f"Running query 4 with parametrs {max_distance=}, {building_type=}, {min_count=}"
    )
    query = f"""
        MATCH p=(:Building {{building:"{building_type}"}})-[e:CLOSE_TO]-(:Building {{building:"{building_type}"}})
        WHERE e.distance <= {max_distance}
        WITH project(p) AS subgraph
        CALL nxalg.strongly_connected_components(subgraph) 
        YIELD components
        UNWIND components as c
        WITH c
        WHERE size(c) >= {min_count}
        RETURN  EXTRACT(n in c | n.id) as ids
    """

    def clusters_of_buidlings_transformation_function(record):
        return record["ids"]

    output_filename = "query4.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Building")
    execute_query("CREATE INDEX ON :Building(building)")
    data = get_query_results_list(query, clusters_of_buidlings_transformation_function)
    result = {
        "max_distance": max_distance,
        "building_type": building_type,
        "min_count": min_count,
        "clusters": data,
    }
    save_object_to_json(result, output_filepath)


def run_query_5(min_angle, max_angle):
    """
    Road/railway crossings; parameters: min angle, max angle
    """
    min_angle = float(min_angle)
    max_angle = float(max_angle)
    print(f"Running query 5 with parametrs {min_angle=}, {max_angle=}")
    query = f"""
    MATCH (railway:Railway)
    MATCH (railway)-[e:CROSSES]->(road:Road)
    WHERE e.angle <= {max_angle} AND e.angle >= {min_angle} 
    RETURN railway.id as railway_id, road.id as road_id, e.angle as angle
    """

    def road_railway_crossings_transformation_function(record):
        return {
            "railway_id": record["railway_id"],
            "road_id": record["road_id"],
            "angle": record["angle"],
        }

    output_filename = "query5.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Railway")
    data = get_query_results_list(query, road_railway_crossings_transformation_function)

    save_object_to_json(data, output_filepath)


def calculate_angle(segment1, segment2):
    """Calculate the angle between two line segments."""

    def vector_angle(v1, v2):
        dot_product = v1[0] * v2[0] + v1[1] * v2[1]
        magnitude1 = math.sqrt(v1[0] ** 2 + v1[1] ** 2)
        magnitude2 = math.sqrt(v2[0] ** 2 + v2[1] ** 2)
        if magnitude1 <= 1e-6 or magnitude2 <= 1e-6:
            return None
        cos_theta = dot_product / (magnitude1 * magnitude2)
        cos_theta = max(-1, min(1, cos_theta))  # Clamp to avoid precision errors
        return math.acos(cos_theta)

    v1 = (segment1[1][0] - segment1[0][0], segment1[1][1] - segment1[0][1])
    v2 = (segment2[1][0] - segment2[0][0], segment2[1][1] - segment2[0][1])

    angle = vector_angle(v1, v2)
    if angle is None:
        return None
    return math.degrees(angle)


def parallel_roads_railways_detection_strict(
    max_distance, max_angle, railway_id, railway_wkt, road_ids, road_wkts
):
    railway = wkt.loads(railway_wkt)
    prepare(railway)
    parallel_road_ids = []

    railway_segments = list(zip(list(railway.coords[:-1]), list(railway.coords[1:])))
    railway_segments = [LineString(rail_seg) for rail_seg in railway_segments]

    for road_wkt, road_id in zip(road_wkts, road_ids):
        road = wkt.loads(road_wkt)

        if railway.distance(road) > max_distance:
            continue

        road_segments = list(zip(list(road.coords[:-1]), list(road.coords[1:])))
        for road_seg in road_segments:
            road_seg_line = LineString(road_seg)

            closest_rail_seg = None
            distance = math.inf
            for rs in railway_segments:
                d = rs.distance(road_seg_line)
                if d <= max_distance and d < distance:
                    closest_rail_seg = rs
                    distance = d
            if closest_rail_seg is None:
                break

            angle = calculate_angle(closest_rail_seg.coords, road_seg)
            if angle is None or not (
                angle <= max_angle or abs(angle - 180) <= max_angle
            ):
                break
        else:
            parallel_road_ids.append(road_id)

    return {"railway_id": railway_id, "parallel_road_ids": parallel_road_ids}


def parallel_roads_railways_detection_lazy(
    max_distance, max_angle, railway_id, railway_wkt, road_ids, road_wkts
):
    railway = wkt.loads(railway_wkt)
    prepare(railway)
    parallel_road_ids = []

    railway_segments = list(zip(list(railway.coords[:-1]), list(railway.coords[1:])))
    railway_segments = [LineString(rail_seg) for rail_seg in railway_segments]

    for road_wkt, road_id in zip(road_wkts, road_ids):
        road = wkt.loads(road_wkt)

        if railway.distance(road) > max_distance:
            continue

        road_segments = list(zip(list(road.coords[:-1]), list(road.coords[1:])))
        for road_seg in road_segments:
            road_seg_line = LineString(road_seg)

            closest_rail_seg = None
            distance = math.inf
            for rs in railway_segments:
                d = rs.distance(road_seg_line)
                if d <= max_distance and d < distance:
                    closest_rail_seg = rs
                    distance = d

            if closest_rail_seg is not None:
                angle = calculate_angle(closest_rail_seg.coords, road_seg)
                if angle is not None and (
                    angle <= max_angle or abs(angle - 180) <= max_angle
                ):
                    parallel_road_ids.append(road_id)
                    break

    return {"railway_id": railway_id, "parallel_road_ids": parallel_road_ids}


def run_query_6(max_distance, max_angle, mode):
    """
    Roads which run parallel to railways; parameters: to be agreed
    """
    max_distance = float(max_distance)
    max_angle = float(max_angle)
    if mode not in ["strict", "lazy"]:
        print("mode should be either 'strict' or 'lazy'")
    print(f"Running query 6 with parametrs {max_distance=}, {max_angle=}, {mode=}")
    if mode == "strict":
        query = f"""
        MATCH (ra:Railway)
        WITH point.distance(ra.upper_right_corner, ra.lower_left_corner) + ({max_distance} * 1.5)  as max_distance, ra, ra.upper_right_corner as p
        MATCH (r:Road)
        WHERE point.distance(r.upper_right_corner, p) <= max_distance AND
        ra.upper_right_corner.x + {max_distance} >= r.lower_left_corner.x AND 
        r.upper_right_corner.x + {max_distance} >= ra.lower_left_corner.x AND 
        ra.upper_right_corner.y + {max_distance} >= r.lower_left_corner.y AND 
        r.upper_right_corner.y + {max_distance} >= ra.lower_left_corner.y
        WITH r, ra
        OPTIONAL MATCH (ra)-[e:CROSSES]->(r)
        WHERE e = NULL
        RETURN ra.id as railway_id, ra.wkt as railway_wkt, COLLECT(r.id) as road_ids, COLLECT(r.wkt) as road_wkts
        """
    else:
        query = f"""
        MATCH (ra:Railway)
        WITH point.distance(ra.upper_right_corner, ra.lower_left_corner) + ({max_distance} * 1.5)  as max_distance, ra, ra.upper_right_corner as p
        MATCH (r:Road)
        WHERE point.distance(r.upper_right_corner, p) <= max_distance AND
        ra.upper_right_corner.x + {max_distance} >= r.lower_left_corner.x AND 
        r.upper_right_corner.x + {max_distance} >= ra.lower_left_corner.x AND 
        ra.upper_right_corner.y + {max_distance} >= r.lower_left_corner.y AND 
        r.upper_right_corner.y + {max_distance} >= ra.lower_left_corner.y
        RETURN ra.id as railway_id, ra.wkt as railway_wkt, COLLECT(r.id) as road_ids, COLLECT(r.wkt) as road_wkts
        """

    def parallel_roads_railways_transformation_function(record):
        return [
            record["railway_id"],
            record["railway_wkt"],
            record["road_ids"],
            record["road_wkts"],
        ]

    output_filename = "query6.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE POINT INDEX ON :Road(upper_right_corner)")
    execute_query("CREATE INDEX ON :Railway")
    data = get_query_results_list(
        query, parallel_roads_railways_transformation_function
    )
    print(f"Recieved {len(data)} railways")

    if mode == "strict":
        function = parallel_roads_railways_detection_strict
    else:
        function = parallel_roads_railways_detection_lazy
    jobs = [(max_distance, max_angle, *args) for args in data]

    result = []
    for _, job_result in parrarelize_processes(function, jobs, n_executors=12):
        result.append(job_result)

    save_object_to_json(result, output_filepath)


def run_query_7(max_distance, min_count):
    """
    Clusters of trees; parameters: max distance, min count; returned as concave hulls
    """
    max_distance = float(max_distance)
    min_count = int(min_count)
    print(f"Running query 7 with parametrs {max_distance=}, {min_count=}")
    query = f"""
        MATCH p=(:Tree)-[e:CLOSE_TO]->(:Tree)
        WHERE e.distance <= {max_distance} 
        WITH project(p) AS subgraph
        CALL nxalg.strongly_connected_components(subgraph) 
        YIELD components
        UNWIND components as c
        WITH c
        WHERE size(c) >= {min_count}
        RETURN EXTRACT(tree in c | [tree.geometry.x, tree.geometry.y]) as trees_x_y
    """

    def tree_clusters_transformation_function(record):
        ch = convex_hull(MultiPoint([(x, y) for x, y in record["trees_x_y"]]))
        return ch.wkt

    output_filename = "query7.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Tree")
    data = get_query_results_list(query, tree_clusters_transformation_function)
    result = {"max_distance": max_distance, "min_count": min_count, "clusters": data}
    save_object_to_json(result, output_filepath)


def run_query_8(start_road_id, end_road_id):
    """
    Shortest path between two indicated roads; parameters: start and end road ids
    """
    start_road_id = int(start_road_id)
    end_road_id = int(end_road_id)
    print(f"Running query 8 with parametrs {start_road_id=}, {end_road_id=}")
    query = f"""
        MATCH (startRoad:Road{{id: {start_road_id}}}), (endRoad:Road{{id: {end_road_id}}})
        WITH startRoad, endRoad
        MATCH (startRoad)<-[:BELONGS_TO]-(startRoadNode:RoadNode{{id: startRoad.start_node_id}})
        MATCH (endRoad)<-[:BELONGS_TO]-(endRoadNode:RoadNode{{id: endRoad.start_node_id}})
        CALL algo.astar(startRoadNode, endRoadNode, {{relationships_filter:["CONNECTED_TO>"]}})
        YIELD path, weight
        RETURN EXTRACT(node in nodes(path) | node.id) as node_ids, weight as distance, startRoad.name as start_name, endRoad.name as end_name; 
    """

    def shortest_path_transformation_function(record):
        return {
            "start": record["start_name"],
            "destination": record["end_name"],
            "distance_meters": record["distance"],
            "path": record["node_ids"],
        }

    output_filename = "query8.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Road(id)")
    data = get_query_results_list(query, shortest_path_transformation_function)
    result = data[0]
    save_object_to_json(result, output_filepath)
    pass


def run_query_9(max_length):
    """
    Quasi-roundabouts: find cycles consisting of one-way streets connected end-to-end; parameters: max length
    """
    max_length = int(max_length)
    print(f"Running query 9 with parametrs {max_length=}")
    query = """
    MATCH p=(r1:Road {oneway:"yes"})-[e:ROAD_CONNECTED_TO {part: "end"}]->(r2:Road {oneway:"yes"})
    WHERE r1.start_node_id != r1.end_node_id AND r2.start_node_id != r2.end_node_id AND r1.end_node_id != r2.end_node_id
    RETURN r1.id as id1, r2.id as id2
    """

    def roundabouts_transformation_function(record):
        return (record["id1"], record["id2"])

    output_filename = "query9.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Road(oneway)")
    data = get_query_results_list(query, roundabouts_transformation_function)

    print(f"Data obtained {len(data)} roads, calculating rounabouts")
    G = nx.DiGraph(data)
    cycles = list(nx.simple_cycles(G, length_bound=max_length))
    result = {"max_length": max_length, "quasi_roundabouts": cycles}
    save_object_to_json(result, output_filepath)


def run_query_10(max_distance, min_count):
    """
    Roads with trees near them; parameters: min count, max distance
    """
    min_count = int(max_distance)
    min_count = int(min_count)
    print(f"Running query 10 with parametrs {max_distance=}, {min_count=}")
    query = f"""
        MATCH (road:Road)
        MATCH p=(tree:Tree)-[e:CLOSE_TO]->(road)
        WHERE e.distance <= {max_distance}
        WITH COLLECT(tree.id) as trees, road
        WHERE size(trees) >= {min_count}
        RETURN road.id as road_id, road.name as road_name, trees as tree_ids
    """

    def road_trees_transformation_function(record):
        return {
            "road_name": record["road_name"],
            "road_id": record["road_id"],
            "trees": record["tree_ids"],
        }

    output_filename = "query10.json"
    output_filepath = f"/data/{output_filename}"

    execute_query("CREATE INDEX ON :Road")
    data = get_query_results_list(query, road_trees_transformation_function)
    save_object_to_json(data, output_filepath)


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
