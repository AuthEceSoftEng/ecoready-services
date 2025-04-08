import tarfile
import io
import os
import docker
from config import settings

def generate_flink_script(
    project_name: str,
    topic_name: str,
    attribute: str,
    every_n: int,
    units: str,
    metric: str,
    interval_type: str,
    sliding_factor: int = None,
    group_by: str = None,
    order_by: str = None
) -> str:
    source_table_sql = f"""
t_env.execute_sql(\"\"\"
CREATE TABLE KafkaSource (
    `key` STRING,
    `{attribute}` DOUBLE,
    `timestamp` STRING,
    `event_time` AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '{settings.kafka_brokers}',
    'key.format' = 'raw',
    'key.fields' = 'key',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'scan.startup.mode' = 'earliest-offset'
)
\"\"\")
"""
    
    sink_topic = f"{topic_name}.{every_n}{units}.{metric}.{attribute}"
    sink_table_sql = f"""
t_env.execute_sql(\"\"\"
CREATE TABLE KafkaSink (
    `key` STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    `count` BIGINT,
    {metric}_{attribute} DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = '{sink_topic}',
    'properties.bootstrap.servers' = '{settings.kafka_brokers}',
    'format' = 'json'
)
\"\"\")
"""
    
    if interval_type == "tumbling":
        window_start_sql = f"TUMBLE_START(`event_time`, INTERVAL '{every_n}' {units.upper()})"
        window_end_sql = f"TUMBLE_END(`event_time`, INTERVAL '{every_n}' {units.upper()})"
        window_sql = f"TUMBLE(`event_time`, INTERVAL '{every_n}' {units.upper()})"
    elif interval_type == "sliding":
        window_start_sql = f"HOP_START(event_time, INTERVAL '{sliding_factor}' {units.upper()}, INTERVAL '{every_n}' {units.upper()})"
        window_end_sql = f"HOP_END(event_time, INTERVAL '{sliding_factor}' {units.upper()}, INTERVAL '{every_n}' {units.upper()})"
        window_sql = f"HOP(event_time, INTERVAL '{sliding_factor}' {units.upper()}, INTERVAL '{every_n}' {units.upper()})"
    
    aggregation_sql = f"""
t_env.execute_sql(\"\"\"
INSERT INTO KafkaSink
SELECT
    `key`,
    {window_start_sql} as window_start,
    {window_end_sql} as window_end,
    COUNT(*) as `count`,
    {metric.upper()}({attribute}) as {metric}_{attribute}
FROM KafkaSource
GROUP BY `key`, {window_sql}
\"\"\")
"""
 
    return f"""
from pyflink.table import TableEnvironment, EnvironmentSettings

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)
table_config = t_env.get_config().set("table.exec.source.idle-timeout", "10000 ms")

{source_table_sql}
{sink_table_sql}
{aggregation_sql}
"""

def deploy_flink_script(script_file_path: str):
    client = docker.from_env()
    container = client.containers.get('test-flink-jobmanager19-1')
    
    with open(script_file_path, 'rb') as script_file:
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tar_info = tarfile.TarInfo(name=os.path.basename(script_file_path))
            tar_info.size = os.path.getsize(script_file_path)
            tar.addfile(tar_info, script_file)
        tar_stream.seek(0)
        container.put_archive('/opt/flink', tar_stream)
    
    exec_log = container.exec_run(
        f'/opt/flink/bin/flink run --python /opt/flink/{os.path.basename(script_file_path)} '
        '--jarfile flink-sql-connector-kafka-3.0.2-1.18.jar'
    )
    return exec_log