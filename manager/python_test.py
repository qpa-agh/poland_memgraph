from neo4j import GraphDatabase
from time import sleep
sleep(3) 

URI = "bolt://memgraph:7687"
AUTH = ("testuser123", "t123")
 
with GraphDatabase.driver(URI, auth=AUTH) as client:
    client.verify_connectivity()
 
    records, summary, keys = client.execute_query(
        "CREATE (u:User {name: $name, password: $password}) RETURN u.name AS name;",
        name="John",
        password="pass",
        database_="memgraph",
    )
 
    for record in records:
        print(record["name"])
 
    print(summary.counters)
 
    records, summary, keys = client.execute_query(
        "MATCH (u:User {name: $name}) RETURN u.name AS name",
        name="John",
        database_="memgraph",
    )
 
    for record in records:
        print(record["name"])
 
    print(summary.query)