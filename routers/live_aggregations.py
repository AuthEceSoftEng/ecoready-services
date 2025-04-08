from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Optional
import uuid
import os
from config import settings
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.flink_utilities import generate_flink_script, deploy_flink_script
from dependencies import check_organization_exists, verify_api_key_access, check_project_exists, verify_master_access

router = APIRouter(dependencies=[Depends(check_organization_exists), Depends(check_project_exists)])
TAG = "Live Aggregations"

@router.post("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}/live-aggregation", 
            tags=[TAG],
            dependencies=[Depends(check_collection_exists), Depends(verify_master_access)])
async def create_live_aggregation(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    attribute: str = Body(..., description="Collection's attribute to perform computation on"),
    stat: str = Body(..., description="Statistical operation to perform", enum=["avg", "median", "max", "min", "sum", "count"]),
    interval: str = Body(..., description="Time interval to group the data by (e.g., every_1_hour)"),
    interval_type: str = Body(..., description="Type of the interval. Can be tumbling or sliding", enum=["tumbling", "sliding"]),
    sliding_factor: Optional[int] = Body(None, description="Sliding factor for the interval, required if interval_type is sliding"),
    group_by: Optional[str] = Body(None, description="Field to group the data by"),
    order_by: Optional[str] = Body(None, description="Field to order the data by and the direction (asc or desc)"),
    api_key: str = Depends(verify_api_key_access)
):
    try:
        # Get names from IDs
        organization = get_organization_by_id(organization_id)
        organization_name = organization.organization_name
        project_name = get_project_by_id(project_id, organization_id).project_name
        collection_name = get_collection_by_id(collection_id, project_id, organization_id).collection_name

        # Define topics
        topic_name = f"{organization_name}.{project_name}.{collection_name}"
        sink_topic = f"{organization_name}.{project_name}.{collection_name}.{interval}.{stat}.{attribute}"

        # Parse and validate interval
        interval_parts = interval.split('_')
        if len(interval_parts) != 3 or interval_parts[0] != "every":
            raise HTTPException(
                status_code=400, 
                detail="Invalid interval format. Expected format: 'every_n_units'"
            )

        every_n = int(interval_parts[1])
        units = interval_parts[2]

        # Validate sliding factor
        if interval_type == "sliding" and not sliding_factor:
            raise HTTPException(
                status_code=400, 
                detail="Sliding factor is required when interval_type is sliding."
            )

        # Generate and deploy Flink script
        script_file_path = f"{organization_name}.{project_name}.{collection_name}_live_aggregation.py"
        try:
            script_content = generate_flink_script(
                project_name=project_name,
                topic_name=topic_name,
                attribute=attribute,
                every_n=every_n,
                units=units,
                metric=stat,
                interval_type=interval_type,
                sliding_factor=sliding_factor,
                group_by=group_by,
                order_by=order_by
            )

            with open(script_file_path, "w") as script_file:
                script_file.write(script_content)

            exec_log = deploy_flink_script(script_file_path)
            
            return {
                "status": "success",
                "log": exec_log.output.decode('utf-8')
            }

        finally:
            # Clean up the script file
            if os.path.exists(script_file_path):
                os.remove(script_file_path)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create live aggregation: {str(e)}"
        )