from neo4j import GraphDatabase
import csv
from multiprocessing import Pool
import os

URI = "bolt://memgraph:7687"
AUTH = ("testuser123", "t123")


def run_with_database_client(func):
    with GraphDatabase.driver(URI, auth=AUTH) as client:
        client.verify_connectivity()
        func(client)


def execute_query(query):
    try:
        with GraphDatabase.driver(URI, auth=AUTH) as client:
            with client.session() as session:
                print("Running query:", query)
                # session.run('STORAGE MODE IN_MEMORY_ANALYTICAL;')
                result = session.run(query)
                print(result.single())
                session.run("FREE MEMORY")

    except BaseException as e:
        print("Failed to execute transaction")
        raise e


def execute_query_to_csv(query, headers, output_file, modifier_function=None, expand_output_list=False):
    """Runs the query and saves the query results to csv."""

    def process_records(tx):
        result = tx.run(query)

        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)

            writer.writerow(headers)
            if modifier_function is None:
                for record in result:
                    writer.writerow(record.values())

            else:
                for record in result:
                    modified_record = modifier_function(record.values())
                    if modified_record is not None:
                        if expand_output_list:
                            writer.writerows(modified_record)
                        else:
                            writer.writerow(modified_record)
                            
        return "finished"

    with GraphDatabase.driver(URI, auth=AUTH) as client:
        client.verify_connectivity()
        with client.session() as session:
            value = session.execute_read(process_records)
            print(value)


def execute_query_to_csv_parallelized(
    query,
    headers,
    output_directory,
    modifier_function=None,
    expand_output_list=False,
    num_processes=10,
    chunk_size=100,
):
    if modifier_function is None:
        return execute_query_to_csv(
            query,
            headers,
            os.path.join(output_directory, "chunk_001.csv"),
            modifier_function=modifier_function,
            expand_output_list=expand_output_list
        )

    os.makedirs(output_directory, exist_ok=True)
    assert num_processes <= 20

    def process_records(tx):
        result_iterator = tx.run(query)

        done = 0
        last_i = 1
        with Pool(processes=num_processes) as pool:
            for chunk_of_chunks in chunked_iterator(
                chunked_iterator(result_iterator, chunk_size, unpack_record=True),
                num_processes,
            ):
                args = []
                for chunk in chunk_of_chunks:
                    args.append(
                        (
                            chunk,
                            os.path.join(output_directory, f"chunk_{last_i:03d}.csv"),
                            headers,
                            modifier_function,
                            expand_output_list
                        )
                    )
                    last_i += 1

                processed_chunks = pool.starmap(process_record_chunk, args)

                for processed_records in processed_chunks:
                    done += processed_records
                    print(done)
            pool.close()
            pool.join()
        return "finished"

    with GraphDatabase.driver(URI, auth=AUTH) as client:
        client.verify_connectivity()
        with client.session() as session:
            value = session.execute_read(process_records)
            print(value)


def process_record_chunk(chunk, output_file, headers, modifier_function, expand_output_list):
    processed = []
    for record in chunk:
        modified_record = modifier_function(record)
        if modified_record is not None:
            if expand_output_list:
                processed.extend(modified_record)
            else:
                processed.append(modified_record)
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(processed)
    return len(processed)


def chunked_iterator(iterator, chunk_size, unpack_record=False):
    chunk = []
    for item in iterator:
        if unpack_record:
            chunk.append(item.values())
        else:
            chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
