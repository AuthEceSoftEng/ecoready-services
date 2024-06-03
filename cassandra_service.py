# cassandra_service.py
from cassandra.cluster import Cluster
from fastapi import HTTPException

class CassandraService:
    def __init__(self):
        self.cluster = Cluster(['localhost'])  # Replace with your Cassandra host
        self.session = self.cluster.connect('eeris_flink_aggregations')  # Your keyspace

    def execute_query(self, query, parameters=None):
        try:
            return self.session.execute(query, parameters)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    def close(self):
        self.cluster.shutdown()
