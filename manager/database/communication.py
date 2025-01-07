from neo4j import GraphDatabase

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
                print('Running query:', query)
                # session.run('STORAGE MODE IN_MEMORY_ANALYTICAL;')
                result = session.run(query)
                print(result.single())
                session.run('FREE MEMORY')
                
    except BaseException as e:
        print("Failed to execute transaction")
        raise e
 
# with GraphDatabase.driver(URI, auth=AUTH) as client:
#     client.verify_connectivity()
 
#     records, summary, keys = client.execute_query(
#         "CREATE (u:User {name: $name, password: $password}) RETURN u.name AS name;",
#         name="John",
#         password="pass",
#         database_="memgraph",
#     )
 
#     for record in records:
#         print(record["name"])
 
#     print(summary.counters)
 
#     records, summary, keys = client.execute_query(
#         "MATCH (u:User {name: $name}) RETURN u.name AS name",
#         name="John",
#         database_="memgraph",
#     )
 
#     for record in records:
#         print(record["name"])
 
#     print(summary.query)

