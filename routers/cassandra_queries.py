from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter, Path
from pydantic import BaseModel
from typing import Dict, Any, List, Union
from dependencies import get_current_user
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
import docker
import json
import os
import pandas as pd
import tarfile
import io
from fastapi.responses import FileResponse
from datetime import datetime, timedelta

TAG = "Queries"
app = FastAPI()
router = APIRouter()

# Cassandra configuration
CASSANDRA_CONTACT_POINTS = ['155.207.19.243']  # Replace with your Cassandra contact points
CASSANDRA_PORT = 9042

def get_cassandra_session():
    cluster = Cluster(CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
    return cluster.connect()


def construct_interval_query(project_name: str, topic_name: str, start_time: str, end_time: str, attribute: str, every_n: int, units: str, stat: str) -> str:
    table_name = f"{project_name}.{topic_name}"

    # Determine the interval in seconds based on the units
    interval_seconds = {
        "minutes": every_n * 60,
        "hours": every_n * 3600,
        "days": every_n * 86400,
        "weeks": every_n * 604800
    }

    if units not in interval_seconds:
        raise HTTPException(status_code=400, detail="Invalid time units. Supported values are: minutes, hours, days, weeks")
    
    interval_value = interval_seconds[units]

    valid_stats = ['avg', 'max', 'min', 'sum', 'count']
    if stat not in valid_stats:
        raise HTTPException(status_code=400, detail=f"Invalid stat. Supported values are: {', '.join(valid_stats)}")

    if stat == 'count':
        stat_query = f"COUNT(*)"
    else:
        stat_query = f"{stat.upper()}({attribute})"
    #SELECT key, dateOf(minTimeuuid(toTimestamp(minTimeuuid(toUnixTimestamp(timestamp) / {interval_value}) * {interval_value}))) AS interval_start, {stat_query} AS {stat}_{attribute}

    query = f"""
    SELECT key, dateOf(minTimeuuid(toUnixTimestamp(timestamp) / {interval_value} * {interval_value})) AS interval_start, {stat_query} AS {stat}_{attribute}
    FROM {table_name}
    """
    
    conditions = []
    if start_time:
        conditions.append(f"timestamp >= '{start_time}'")
    if end_time:
        conditions.append(f"timestamp <= '{end_time}'")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " GROUP BY key, dateOf(minTimeuuid(toUnixTimestamp(timestamp) / {interval_value} * {interval_value})) ALLOW FILTERING"
    
    return query

def get_raw_data(
    project_name: str,
    topic: str,
    attribute: str,
    start_time: str = Query(None, description="Start time in format YYYY-MM-DD HH:MM:SS"),
    end_time: str = Query(None, description="End time in format YYYY-MM-DD HH:MM:SS"),
    user: dict = Depends(get_current_user)
):
    cassandra_session = get_cassandra_session()
    table_name = f"{project_name}.{topic}"

    query = f"SELECT key, timestamp, {attribute} FROM {table_name}"

    conditions = []
    if start_time:
        conditions.append(f"timestamp >= '{start_time}'")
    if end_time:
        conditions.append(f"timestamp <= '{end_time}'")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ALLOW FILTERING"

    try:
        statement = SimpleStatement(query)
        result = cassandra_session.execute(statement)
        result_list = list(result)
        return {"result": [dict(row._asdict()) for row in result_list]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")


def get_interval_start(timestamp, start_time, interval_unit, interval_value=1):
    if interval_unit == 'minutes':
        # Align to the start of the minute interval
        total_minutes = (timestamp - start_time).total_seconds() // 60
        interval_start_minutes = total_minutes - (total_minutes % interval_value)
        return start_time + timedelta(minutes=interval_start_minutes)
    elif interval_unit == 'hours':
        # Align to the start of the hour interval
        total_hours = (timestamp - start_time).total_seconds() // 3600
        interval_start_hours = total_hours - (total_hours % interval_value)
        return start_time + timedelta(hours=interval_start_hours)
    elif interval_unit == 'days':
        # Align to the start of the day interval
        total_days = (timestamp - start_time).days
        interval_start_days = total_days - (total_days % interval_value)
        return start_time + timedelta(days=interval_start_days)
    elif interval_unit == 'weeks':
        # Align to the start of the week interval
        total_weeks = (timestamp - start_time).days // 7
        interval_start_weeks = total_weeks - (total_weeks % interval_value)
        return start_time + timedelta(weeks=interval_start_weeks)
    elif interval_unit == 'months':
        # Align to the start of the month interval
        month_diff = (timestamp.year - start_time.year) * 12 + (timestamp.month - start_time.month)
        interval_start_months = month_diff - (month_diff % interval_value)
        start_month = (start_time.month + interval_start_months) % 12 or 12
        start_year = start_time.year + (start_time.month + interval_start_months - 1) // 12
        return datetime(start_year, start_month, 1)
    else:
        raise ValueError(f"Unsupported interval unit: {interval_unit}")
def aggregate_data(data, interval_value, interval_unit, stat, attribute):
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Find the earliest timestamp for each key
    earliest_times = df.groupby('key')['timestamp'].min().to_dict()

    # Calculate the interval start times
    df['interval_start'] = df.apply(lambda row: get_interval_start(row['timestamp'], earliest_times[row['key']], interval_unit, interval_value), axis=1)

    if stat == 'avg':
        grouped = df.groupby(['key', 'interval_start']).agg({attribute: 'mean'}).reset_index()
    elif stat == 'max':
        grouped = df.groupby(['key', 'interval_start']).agg({attribute: 'max'}).reset_index()
    elif stat == 'min':
        grouped = df.groupby(['key', 'interval_start']).agg({attribute: 'min'}).reset_index()
    elif stat == 'sum':
        grouped = df.groupby(['key', 'interval_start']).agg({attribute: 'sum'}).reset_index()
    elif stat == 'count':
        grouped = df.groupby(['key', 'interval_start']).agg({attribute: 'count'}).reset_index()
    else:
        raise ValueError(f"Unsupported statistical operation: {stat}")

    return grouped.to_dict(orient='records')

# @router.get("/projects/{project_name}/{topic}/data/")
# async def get_data(
#     project_name: str,
#     topic: str,
#     filter_column: str = Query(None, description="Column name to apply the filter on"),
#     filter_value: Union[str, int, float] = Query(None, description="Value to filter the column"),
#     comparison: str = Query("=", enum=["<", ">", "<=", ">=", "="], description="Comparison operator to use: <, >, <=, >=, ="),
#     start_time: str = Query(None, description="Start time in format YYYY-MM-DD HH:MM:SS"),
#     end_time: str = Query(None, description="End time in format YYYY-MM-DD HH:MM:SS"),
#     user: dict = Depends(get_current_user)
# ):
#     cassandra_session = get_cassandra_session()

#     # Validate comparison operator if filter_column and filter_value are provided
#     if filter_column and filter_value is not None and comparison not in ["<", ">", "<=", ">=", "="]:
#         raise HTTPException(status_code=400, detail="Invalid comparison operator. Supported operators: <, >, <=, >=, =")

#     # Construct the query
#     table_name = f"{project_name}.{topic}"
#     query = f"SELECT * FROM {table_name}"

#     # Add filtering conditions
#     conditions = []
#     if filter_column and filter_value is not None:
#         formatted_value = format_value(filter_value)
#         conditions.append(f"{filter_column} {comparison} {formatted_value}")

#     if start_time:
#         conditions.append(f"timestamp >= '{start_time}'")
    
#     if end_time:
#         conditions.append(f"timestamp <= '{end_time}'")

#     if conditions:
#         query += " WHERE " + " AND ".join(conditions)

#     query += " ALLOW FILTERING"

#     # Execute the query
#     try:
#         statement = SimpleStatement(query)
#         result = cassandra_session.execute(statement)
#         result_list = list(result)
        
#         # Exclude the "day" column from the results
#         formatted_result = [{k: v for k, v in row._asdict().items() if k != 'day'} for row in result_list]

#         return {"result": formatted_result}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")

# @router.get("/projects/{project_name}/{topic}/stats/")
# async def calculate_stats(
#     project_name: str,
#     topic: str,
#     stat: str = Query(..., description="Statistical operation to perform: max, min, avg, sum, count"),
#     attribute: str = Query(None, description="Attribute to perform the statistical operation on. Not required for count."),
#     group_by: str = Query(None, description="Attribute to group by"),
#     start_time: str = Query(None, description="Start time in format YYYY-MM-DD HH:MM:SS"),
#     end_time: str = Query(None, description="End time in format YYYY-MM-DD HH:MM:SS"),
#     user: dict = Depends(get_current_user)
# ):
#     cassandra_session = get_cassandra_session()

#     # Validate attribute for operations other than count
#     if stat != 'count' and attribute is None:
#         raise HTTPException(status_code=400, detail="Attribute is required for the specified statistical operation")

#     # Construct the query
#     try:
#         query = construct_time_range_query(project_name, topic, start_time, end_time, stat, attribute, group_by)
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

#     # Execute the query
#     try:
#         statement = SimpleStatement(query)
#         result = cassandra_session.execute(statement)
#         result_list = list(result)

#         if group_by:
#             formatted_result = [{group_by: getattr(row, group_by), f"{stat}_{attribute}": getattr(row, f"{stat}_{attribute}")} for row in result_list]
#         else:
#             formatted_result = [getattr(row, f"{stat}_{attribute}") for row in result_list]

#         return {"result": formatted_result}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")
def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def format_value(value):
    if isinstance(value, str):
        value = value.strip("\"'")
        if is_number(value):
            return value
        return "'{}'".format(value)
    return value

@router.get("/projects/{project_name}/{topic}/data/", tags=[TAG])
async def get_data(
    project_name: str,
    topic: str,
    attributes: str = Query(None, description="Comma-separated list of attributes to retrieve, e.g., 'temperature,precipitation'"),
    filters: str = Query(None, description="Comma-separated list of filters in the form 'column1>10,column2<40'"),
    start_time: str = Query(None, description="Start time in format YYYY-MM-DD HH:MM:SS"),
    end_time: str = Query(None, description="End time in format YYYY-MM-DD HH:MM:SS"),
    user: dict = Depends(get_current_user)
):
    cassandra_session = get_cassandra_session()

    # Construct the query
    table_name = f"{project_name}.{topic}"

    # Parse attributes to select
    if attributes:
        try:
            attributes_list = [attr.strip() for attr in attributes.split(',')]
            select_columns = ", ".join(attributes_list)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid format for attributes")
    else:
        select_columns = "*"

    query = f"SELECT {select_columns} FROM {table_name}"

    # Add filtering conditions
    conditions = []

    if filters:
        try:
            filters_list = filters.split(',')
            for filter_str in filters_list:
                filter_str = filter_str.strip()
                if '>=' in filter_str:
                    column, value = filter_str.split('>=', 1)
                    operator = '>='
                elif '<=' in filter_str:
                    column, value = filter_str.split('<=', 1)
                    operator = '<='
                elif '>' in filter_str:
                    column, value = filter_str.split('>', 1)
                    operator = '>'
                elif '<' in filter_str:
                    column, value = filter_str.split('<', 1)
                    operator = '<'
                elif '=' in filter_str:
                    column, value = filter_str.split('=', 1)
                    operator = '='
                else:
                    raise ValueError(f"Invalid filter format: {filter_str}")
                
                column = column.strip()
                value = value.strip()
                formatted_value = format_value(value)
                conditions.append(f"{column} {operator} {formatted_value}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    if start_time:
        conditions.append(f"timestamp >= '{start_time}'")
    
    if end_time:
        conditions.append(f"timestamp <= '{end_time}'")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ALLOW FILTERING"

    # Execute the query
    try:
        statement = SimpleStatement(query)
        result = cassandra_session.execute(statement)
        result_list = list(result)
        
        # Exclude the "day" column from the results, if it exists
        formatted_result = [{k: v for k, v in row._asdict().items() if k != 'day'} for row in result_list]

        return {"result": formatted_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")


@router.get("/projects/{project_name}/{topic}/stats/", tags=[TAG])
async def calculate_interval_stat(
    project_name: str,
    topic: str,
    attribute: str = Query("temperature", description="Attribute to calculate the stat for"),
    every_n: int = Query(..., description="Number of units for the interval"),
    units: str = Query("days", enum=["minutes", "hours", "days", "weeks", "months"], description="Units for the interval"),
    stat: str = Query("avg", enum=["avg", "max", "min", "sum", "count"], description="Statistical operation to perform"),
    start_time: str = Query(None, description="Start time in format YYYY-MM-DD HH:MM:SS"),
    end_time: str = Query(None, description="End time in format YYYY-MM-DD HH:MM:SS"),
    user: dict = Depends(get_current_user)
):
    raw_data_response = get_raw_data(project_name, topic, attribute, start_time, end_time, user)
    raw_data = raw_data_response['result']
    try:
        aggregated_data = aggregate_data(raw_data, every_n, units, stat, attribute)
        return {"result": aggregated_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to aggregate data: {str(e)}")



@router.get("/projects/{project_name}/{topic}/first_10_unix_timestamps/", tags=[TAG])
async def get_first_10_unix_timestamps(
    project_name: str,
    topic: str,
    user: dict = Depends(get_current_user)
):
    cassandra_session = get_cassandra_session()
    table_name = f"{project_name}.{topic}"

    query = f"""
    SELECT dateOf(minTimeuuid(toUnixTimestamp(prod_timestamp) / 30000 * 30000)) AS  unix_timestamp
    FROM {table_name}
    LIMIT 10
    """
    
    # Execute the query
    try:
        statement = SimpleStatement(query)
        result = cassandra_session.execute(statement)
        result_list = list(result)

        formatted_result = [{"unix_timestamp": row.unix_timestamp} for row in result_list]

        return {"result": formatted_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")



@router.get("/projects/{project_name}/{topic}/real-time-aggregation", tags=[TAG])
async def generate_script(
    project_name: str = Path(..., description="Name of the project"),
    topic: str = Path(..., description="Kafka topic"),
    attribute: str = Query(..., description="Attribute of interest"),
    every_n: int = Query(..., description="Aggregation window size"),
    units: str = Query("days", enum=["minutes", "hours", "days", "weeks", "months"], description="Units for the interval"),
    metric: str = Query("avg", enum=["avg", "max", "min", "sum", "count"], description="Statistical operation to perform"),
):
    full_topic = f"{project_name}.{topic}"
    sink_topic = f"{project_name}.{topic}.{every_n}{units}.{metric}.{attribute}"
    
    # Define the source table SQL dynamically
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
    'topic' = '{full_topic}',
    'properties.bootstrap.servers' = '155.207.19.243:19092',
    'key.format' = 'raw',
    'key.fields' = 'key',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'scan.startup.mode' = 'earliest-offset'
)
\"\"\")
"""
    
    # Define the sink table SQL dynamically
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
    'properties.bootstrap.servers' = '155.207.19.243:19092',
    'format' = 'json'
)
\"\"\")
"""
    
    # Construct the Flink SQL query dynamically
    aggregation_sql = f"""
t_env.execute_sql(\"\"\"
INSERT INTO KafkaSink
SELECT
    `key`,
    TUMBLE_START(`event_time`, INTERVAL '{every_n}' {units.upper()}) as window_start,
    TUMBLE_END(`event_time`, INTERVAL '{every_n}' {units.upper()}) as window_end,
    COUNT(*) as `count`,
    {metric.upper()}({attribute}) as {metric}_{attribute}
FROM KafkaSource
GROUP BY `key`, TUMBLE(`event_time`, INTERVAL '{every_n}' {units.upper()})
\"\"\")
"""

    # Combine everything into a Python script
    script_content = f"""
from pyflink.table import TableEnvironment, EnvironmentSettings

# Set up the environment for streaming processing
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)
table_config = t_env.get_config().set("table.exec.source.idle-timeout", "10000 ms")

# Define a Kafka source table
{source_table_sql}

# Define a Kafka sink table
{sink_table_sql}

# Perform aggregations and write to Kafka sink
{aggregation_sql}
"""
    # Write the script to a file
    script_file_path = f"{project_name}.{topic}_aggregation.py"
    with open(script_file_path, "w") as script_file:
        script_file.write(script_content)
    client = docker.from_env()
    try:
        container = client.containers.get('test-flink-jobmanager-1')  # Replace with your JobManager container name
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tar.add(script_file_path, arcname=os.path.basename(script_file_path))
        tar_stream.seek(0)
        container.put_archive('/opt/flink', tar_stream)
        exec_log = container.exec_run(f'/opt/flink/bin/flink run --python /opt/flink/{os.path.basename(script_file_path)}  --jarfile flink-sql-connector-kafka-3.0.2-1.18.jar')
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="JobManager container not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"status": "success", "log": exec_log.output.decode('utf-8')}
    
    
    # Return the script file as a response
    return FileResponse(script_file_path, media_type='application/octet-stream', filename=script_file_path)

