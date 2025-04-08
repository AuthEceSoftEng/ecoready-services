# app/utilities/cassandra.py

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.query import SimpleStatement
from config import settings

def get_cassandra_session():
    execution_profile = ExecutionProfile(
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),  # Replace with your actual datacenter name
    request_timeout=60.0,  # Increase the timeout to 60 seconds
    retry_policy=RetryPolicy()  # Optional: Retry policy for failed requests
    )

    cluster = Cluster([settings.cassandra_contact_points], port=settings.cassandra_port,
                      protocol_version=5, execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile})
    session = cluster.connect('metadata')  # Make sure the keyspace is set correctly
    return session
