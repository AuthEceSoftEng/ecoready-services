from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter
from pydantic import BaseModel
from typing import Dict, Any, List, Union
from dependencies import get_current_user
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time

app = FastAPI()
router = APIRouter()

# Cassandra configuration
CASSANDRA_CONTACT_POINTS = ['155.207.19.242']  # Replace with your Cassandra contact points
CASSANDRA_PORT = 9042

def get_cassandra_session():
    cluster = Cluster(CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
    return cluster.connect()

def construct_time_range_query(project_name: str, topic_name: str, start_time: str, end_time: str, stat: str, attribute: str, group_by: str) -> str:
    table_name = f"{project_name}.{topic_name}"
    if stat == None:
        base_query = f"SELECT * FROM {table_name} WHERE prod_timestamp >= '{start_time}' AND prod_timestamp <= '{end_time}' ALLOW FILTERING"
        return base_query
    
    if stat not in ['max', 'min', 'avg']:
        raise HTTPException(status_code=400, detail=f"Unsupported statistical operation: {stat}")
    
    base_query = f"SELECT {stat}({attribute}) AS {stat}_{attribute}"
    if group_by:
        base_query += f", {group_by}"

    base_query += f" FROM {table_name} WHERE prod_timestamp >= '{start_time}' AND prod_timestamp <= '{end_time}'"

    if group_by:
        base_query += f" GROUP BY {group_by}"

    base_query += " ALLOW FILTERING"
    return base_query

@router.get("/projects/{project_name}/{topic}/query/")
async def query_data(
    project_name: str,
    topic: str,
    start_time: str = Query(..., description="Start time in format YYYY-MM-DD HH:MM:SS"),
    end_time: str = Query(..., description="End time in format YYYY-MM-DD HH:MM:SS"),
    stat: str = Query(None, description="Statistical operation to perform: max, min, avg"),
    attribute: str = Query(None, description="Attribute to perform the statistical operation on"),
    group_by: str = Query(None, description="Attribute to group by"),
    user: dict = Depends(get_current_user)
):
    cassandra_session = get_cassandra_session()

    # Construct the query
    try:
        query = construct_time_range_query(project_name, topic, start_time, end_time, stat, attribute, group_by)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Execute the query
    try:
        statement = SimpleStatement(query)
        result = cassandra_session.execute(statement)
        result_list = list(result)
        if stat == None:
            formatted_result = [dict(row._asdict()) for row in result_list]
        elif group_by:
            formatted_result = [{group_by: getattr(row, group_by), f"{stat}_{attribute}": getattr(row, f"{stat}_{attribute}")} for row in result_list]
        else:
            formatted_result = [getattr(row, f"{stat}_{attribute}") for row in result_list]

        return {"result": formatted_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")

