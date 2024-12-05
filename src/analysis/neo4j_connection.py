from neo4j import GraphDatabase


class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_venue_node(self, venue_data):
        with self.driver.session() as session:
            query = """
            CREATE (v:Venue {
                name: $name,
                location: $location,
                rating: $rating
            })
            """
            session.run(query, **venue_data)

    def create_user_node(self, user_data):
        with self.driver.session() as session:
            query = """
            CREATE (u:User {
                username: $username,
                karma: $karma
            })
            """
            session.run(query, **user_data)
