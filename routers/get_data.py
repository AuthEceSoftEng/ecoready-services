from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.collection_utils import get_collection_by_id
from utilities.cassandra_connector import get_cassandra_session
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from dependencies import check_organization_exists, check_project_exists, verify_api_key_access, verify_endpoint_access, verify_write_access
from utilities.collection_utils import check_collection_exists
from typing import List, Dict, Union
import uuid
from config import settings
from confluent_kafka import Producer
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.collection_utils import get_collection_by_id
from datetime import datetime
import json 
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Any, Dict
from pydantic import BaseModel
import json
import uuid
from datetime import datetime
from dependencies import generate_filter_condition


session = get_cassandra_session()


router = APIRouter(dependencies=[Depends(check_organization_exists), Depends(check_project_exists)])
TAG = "Get Data"

class Filter(BaseModel):
    property_name: str
    operator: str
    property_value: Any
    operands: Optional[List['Filter']] = None

class OrderBy(BaseModel):
    field: str
    order: str  # 'asc' or 'desc'

@router.get("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}/get_data", 
            tags=[TAG], 
            dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)])
async def get_data_from_collection(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    attributes: List[str] = Query(None),
    filters: Optional[str] = Query(None),
    order_by: Optional[str] = Query(None)
):
    # Get names from IDs
    organization_name = get_organization_by_id(organization_id).organization_name
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
    # Validate attributes
    if attributes:
        invalid_attrs = [attr for attr in attributes if attr not in schema]
        if invalid_attrs:
            raise HTTPException(
                status_code=422, 
                detail=f"Attributes not in schema: {', '.join(invalid_attrs)}"
            )
    
    # Build query
    select_clause = ', '.join(attributes) if attributes else '*'
    query = f"SELECT {select_clause} FROM {keyspace_name}.{table_name}"
    
    # Handle filters
    conditions = []
    if filters:
        try:
            filters_list = json.loads(filters)
            for f in filters_list:
                if f['operator'].lower() == "or":
                    or_conditions = []
                    for operand in f["operands"]:
                        or_conditions.append(
                            generate_filter_condition(
                                operand["property_name"], 
                                operand["operator"], 
                                operand["property_value"]
                            )
                        )
                    conditions.append(f"({' OR '.join(or_conditions)})")
                else:
                    conditions.append(
                        generate_filter_condition(
                            f['property_name'], 
                            f['operator'], 
                            f['property_value']
                        )
                    )
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=422, 
                detail="Invalid filter format"
            )
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ALLOW FILTERING"
    
    # Execute query
    try:
        results = session.execute(query)
        results_list = [dict(row._asdict()) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )
    
    # Handle ordering
    if order_by:
        try:
            order_by_dict = json.loads(order_by)
            if order_by_dict['field'] not in schema:
                raise HTTPException(
                    status_code=422,
                    detail=f"Order field '{order_by_dict['field']}' not in schema"
                )
            results_list.sort(
                key=lambda x: x.get(order_by_dict['field']),
                reverse=(order_by_dict['order'].lower() == "desc")
            )
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=422,
                detail="Invalid order_by format"
            )
    
    return results_list
