from fastapi import APIRouter, Depends, HTTPException
import uuid
import docker
from config import settings
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from dependencies import check_organization_exists, verify_api_key_access, check_project_exists, verify_endpoint_access

router = APIRouter(dependencies=[Depends(check_organization_exists), Depends(check_project_exists)])
TAG = "Get Collection Live Data"

@router.post("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}/live_data", 
            tags=[TAG],
            dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)])
async def start_live_data_consumer(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    api_key: str = Depends(verify_api_key_access)
):
    try:
        # Get names from IDs
        organization = get_organization_by_id(organization_id)
        organization_name = organization.organization_name
        project_name = get_project_by_id(project_id, organization_id).project_name
        collection_name = get_collection_by_id(collection_id, project_id, organization_id).collection_name

        # Create container name and topic name
        container_name = f"{organization_name}_{project_name}_{collection_name}_{api_key}_live_consumer"

        # Initialize Docker client
        docker_client = docker.from_env()

        # Run the Docker container for the Kafka consumer
        docker_client.containers.run(
            "kafka-live-consumer",
            name=container_name,
            environment={
                "KAFKA_BOOTSTRAP_SERVERS": settings.kafka_brokers,
                "CASSANDRA_CONTACT_POINTS": settings.cassandra_contact_points,
                "CASSANDRA_PORT": str(settings.cassandra_port),
                "COLLECTION_NAME": collection_name,
                "PROJECT_NAME": project_name,
                "ORGANIZATION_NAME": organization_name,
                "GROUP_ID": f"{collection_name}.{project_name}_{api_key}_live_group"
            },
            detach=True,
            restart_policy={"Name": "always"}
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to start consumer container: {str(e)}"
        )

    return {
        "message": f"Consumer container started for collection '{collection_id}'."
    }

@router.delete("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}/live_data", 
             tags=[TAG],
             dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)])
async def stop_live_data_consumer(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    api_key: str = Depends(verify_api_key_access)
):
    try:
        # Get names from IDs
        organization = get_organization_by_id(organization_id)
        organization_name = organization.organization_name
        project_name = get_project_by_id(project_id, organization_id).project_name
        collection_name = get_collection_by_id(collection_id, project_id, organization_id).collection_name

        # Create container name
        container_name = f"{organization_name}_{project_name}_{collection_name}_{api_key}_live_consumer"

        # Initialize Docker client
        docker_client = docker.from_env()

        # Try to get and stop the container
        try:
            container = docker_client.containers.get(container_name)
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            raise HTTPException(
                status_code=404,
                detail=f"Live data consumer container not found for the collection '{collection_id}'"
            )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop consumer container: {str(e)}"
        )

    return {
        "message": f"Consumer container stopped for collection '{collection_id}'."
    }