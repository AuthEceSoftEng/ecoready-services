from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Any, Dict
import uuid
import json
import pandas as pd
from datetime import datetime
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.collection_utils import get_collection_by_id
from dependencies import generate_filter_condition, aggregate_data
from dependencies import check_organization_exists, check_project_exists, verify_api_key_access, verify_endpoint_access, verify_write_access
from utilities.collection_utils import check_collection_exists
from utilities.cassandra_connector import get_cassandra_session
session = get_cassandra_session()

router = APIRouter(dependencies=[Depends(check_organization_exists), Depends(check_project_exists)])
TAG = "Get Data Statistics"

@router.get("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}/statistics", 
            tags=[TAG], 
            dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)])
async def get_collection_statistics(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    attribute: str,
    stat: str = Query("avg", enum=["avg", "max", "min", "sum", "count"], description="Statistical operation to perform"),
    interval: str = "every_2_days",
    start_time: str = Query(None, description="Start time in format YYYY-MM-DDTHH:MM:SSZ"),
    end_time: str = Query(None, description="End time in format YYYY-MM-DDTHH:MM:SSZ"),
    filters: Optional[str] = Query(None),
    order: Optional[str] = Query(None, enum=["asc", "desc"]),
    group_by: Optional[str] = Query(None)
):
    # Get names from IDs
    organization = get_organization_by_id(organization_id)
    organization_name = organization.organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection_name = get_collection_by_id(collection_id, project_id, organization_id).collection_name

    # Form the keyspace and table names
    keyspace_name = organization_name.lower()
    table_name = f"{project_name.lower()}_{collection_name.lower()}"

    # Get schema information
    schema_query = """
    SELECT column_name, type 
    FROM system_schema.columns 
    WHERE keyspace_name=%s AND table_name=%s
    """
    rows = session.execute(schema_query, (keyspace_name, table_name))
    schema = {row.column_name: row.type for row in rows}

    # Validate attribute
    if attribute not in schema:
        raise HTTPException(status_code=422, detail=f"Attribute {attribute} does not exist in the collection schema.")
    
    if group_by and group_by not in schema:
        raise HTTPException(status_code=422, detail=f"Group by field {group_by} does not exist in the collection schema.")

    # Default group_by to 'key' if not provided
    group_by = group_by or 'key'

    # Build the SELECT statement
    query = f"SELECT {group_by}, timestamp, {attribute} FROM {keyspace_name}.{table_name}"

    # Add filters to the query
    conditions = []
    if filters:
        filters_list = json.loads(filters)
        for f in filters_list:
            if f['operator'] == "or":
                or_conditions = []
                for operand in f["operands"]:
                    or_conditions.append(generate_filter_condition(operand["property_name"], operand["operator"], operand["property_value"]))
                conditions.append(f"({' OR '.join(or_conditions)})")
            else:
                conditions.append(generate_filter_condition(f['property_name'], f['operator'], f['property_value']))

    # Add WHERE conditions if there are any
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    # Add time conditions
    if start_time and end_time:
        if not conditions:
            query += f" WHERE timestamp >= '{start_time}' AND timestamp <= '{end_time}'"
        else:
            query += f" AND timestamp >= '{start_time}' AND timestamp <= '{end_time}'"

    query += " ALLOW FILTERING"

    # Execute the query and fetch data
    try:
        results = session.execute(query)
        results_list = [dict(row._asdict()) for row in results]

        # Group and aggregate data based on the specified interval
        if interval:
            _, every_n, units = interval.split('_')
            every_n = int(every_n)
            if units not in ["minutes", "hours", "days", "weeks", "months"]:
                raise HTTPException(status_code=422, detail=f"The unit for the interval isn't supported")
            aggregated_data = aggregate_data(results_list, every_n, units, stat, attribute, group_by)
        else:
            aggregated_data = results_list

    except Exception as e:
        print("ERROR!")
        print("---------------------------")
        print(e)
        return []

    if order:
        aggregated_data.sort(key=lambda x: x.get(f"{stat}_{attribute}"), reverse=(order == "desc"))

    return aggregated_data