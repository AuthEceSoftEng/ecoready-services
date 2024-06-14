
from pyflink.table import TableEnvironment, EnvironmentSettings

# Set up the environment for streaming processing
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)
table_config = t_env.get_config().set("table.exec.source.idle-timeout", "10000 ms")

# Define a Kafka source table

t_env.execute_sql("""
CREATE TABLE KafkaSource (
    `key` STRING,
    `temperature` DOUBLE,
    `timestamp` STRING,
    `event_time` AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'M_AGROLAB.M_AGROLAB_TOPIC',
    'properties.bootstrap.servers' = '155.207.19.243:19092',
    'key.format' = 'raw',
    'key.fields' = 'key',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'scan.startup.mode' = 'earliest-offset'
)
""")


# Define a Kafka sink table

t_env.execute_sql("""
CREATE TABLE KafkaSink (
    `key` STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    `count` BIGINT,
    avg_temperature DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'M_AGROLAB.M_AGROLAB_TOPIC.5seconds.avg.temperature',
    'properties.bootstrap.servers' = '155.207.19.243:19092',
    'format' = 'json'
)
""")


# Perform aggregations and write to Kafka sink

t_env.execute_sql("""
INSERT INTO KafkaSink
SELECT
    `key`,
    TUMBLE_START(`event_time`, INTERVAL '5' SECONDS) as window_start,
    TUMBLE_END(`event_time`, INTERVAL '5' SECONDS) as window_end,
    COUNT(*) as `count`,
    AVG(temperature) as avg_temperature
FROM KafkaSource
GROUP BY `key`, TUMBLE(`event_time`, INTERVAL '5' SECONDS)
""")

