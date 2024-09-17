
from pyflink.table import TableEnvironment, EnvironmentSettings

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)
table_config = t_env.get_config().set("table.exec.source.idle-timeout", "10000 ms")


t_env.execute_sql("""
CREATE TABLE KafkaSource (
    `key` STRING,
    `c` DOUBLE,
    `timestamp` STRING,
    `event_time` AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'org.project.collection',
    'properties.bootstrap.servers' = '155.207.19.243:19496',
    'key.format' = 'raw',
    'key.fields' = 'key',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'scan.startup.mode' = 'earliest-offset'
)
""")


t_env.execute_sql("""
CREATE TABLE KafkaSink (
    `key` STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    `count` BIGINT,
    avg_c DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'org.project.collection.2minutes.avg.c',
    'properties.bootstrap.servers' = '155.207.19.243:19496',
    'format' = 'json'
)
""")


t_env.execute_sql("""
INSERT INTO KafkaSink
SELECT
    `key`,
    TUMBLE_START(`event_time`, INTERVAL '2' MINUTES) as window_start,
    TUMBLE_END(`event_time`, INTERVAL '2' MINUTES) as window_end,
    COUNT(*) as `count`,
    AVG(c) as avg_c
FROM KafkaSource
GROUP BY `key`, TUMBLE(`event_time`, INTERVAL '2' MINUTES)
""")

