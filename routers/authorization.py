from fastapi import APIRouter, HTTPException, Depends, Query
from dependencies import get_current_user
from typing import Dict, Any
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, AclBinding, AclBindingFilter, AclOperation, AclPermissionType, ResourceType, ResourcePatternType

TAG = "Authorization"

router = APIRouter()


bootstrap_servers = 'localhost:59092'
security_protocol = 'SASL_PLAINTEXT'
sasl_mechanisms = 'PLAIN'
def acl_binding_to_dict(acl_binding):
    operation_map = {
        AclOperation.ANY: "ANY",
        AclOperation.ALL: "ALL",
        AclOperation.READ: "READ",
        AclOperation.WRITE: "WRITE",
        AclOperation.CREATE: "CREATE",
        # Add other operations as needed
    }
    permission_type_map = {
        AclPermissionType.ANY: "ANY",
        AclPermissionType.DENY: "DENY",
        AclPermissionType.ALLOW: "ALLOW",
        # Add other permission types as needed
    }
    acl_dict = {
        "restype": acl_binding.restype.name,
        "name": acl_binding.name,
        "resource_pattern_type": acl_binding.resource_pattern_type.name,
        "principal": acl_binding.principal,
        "host": acl_binding.host,
        "operation": operation_map.get(acl_binding.operation, "UNKNOWN"),
        "permission_type": permission_type_map.get(acl_binding.permission_type, "UNKNOWN"),
    }
    return acl_dict
@router.post("/set-topic-acl/")
async def set_topic_acl(topic_name: str, username: str,  operation: str = Query('READ', enum=['READ', 'WRITE', 'ALTER_CONFIGS', 'DESCRIBE_CONFIGS']) , user: dict = Depends(get_current_user)):
    kafka_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanisms': sasl_mechanisms,
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    }
    admin_client = AdminClient(kafka_conf)
    operation_map = {
        'READ': AclOperation.READ,
        'WRITE': AclOperation.WRITE,
        'ALTER_CONFIGS': AclOperation.ALTER_CONFIGS,
        'DESCRIBE_CONFIGS': AclOperation.DESCRIBE_CONFIGS,
    }
    acl_operation = operation_map[operation]
    acl_binding = AclBinding(
        restype=ResourceType.TOPIC,
        name=topic_name,
        resource_pattern_type=ResourcePatternType.LITERAL,
        principal=f'User:{username}',
        host='*',
        operation=acl_operation,
        permission_type=AclPermissionType.ALLOW
    )
    # Create the ACL
    try:
        futures = admin_client.create_acls([acl_binding])
        for acl, future in futures.items():
            future.result()  # The result is None if successful
        return {"message": f"ACL added successfully for {username} on topic {topic_name}."}
    except Exception as e:
        if e.args[0].code() == KafkaError.CLUSTER_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail=f"Authorization failed. You are not allowed to set ACLs.")
        else:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
@router.post("/set-acl/")
async def set_acl(resource_name: str, username: str, resource_type: str = Query('TOPIC', enum=['ANY', 'TOPIC', 'GROUP', 'BROKER']),  resource_pattern: str = Query('LITERAL', enum=['ANY', 'MATCH', 'LITERAL', 'PREFIXED']) , operation: str = Query('READ', enum=['READ', 'WRITE','ANY', 'ALL','CREATE','DELETE','ALTER','DESCRIBE','CLUSTER_ACTION', 'ALTER_CONFIGS', 'DESCRIBE_CONFIGS','IDEMPOTENT_WRITE']) , permission: str = Query('ALLOW', enum=['ANY', 'ALLOW', 'DENY']) , user: dict = Depends(get_current_user)):
    kafka_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanisms': sasl_mechanisms,
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    }
    admin_client = AdminClient(kafka_conf)
    operation_map = {
        'UNKNOWN': AclOperation.UNKNOWN,
        'ANY': AclOperation.ANY,
        'ALL': AclOperation.ALL,
        'READ': AclOperation.READ,
        'WRITE': AclOperation.WRITE,
        'CREATE': AclOperation.CREATE,
        'DELETE': AclOperation.DELETE,
        'ALTER': AclOperation.ALTER,
        'DESCRIBE': AclOperation.DESCRIBE,
        'CLUSTER_ACTION': AclOperation.CLUSTER_ACTION,
        'DESCRIBE_CONFIGS': AclOperation.DESCRIBE_CONFIGS,
        'ALTER_CONFIGS': AclOperation.ALTER_CONFIGS,
        'IDEMPOTENT_WRITE': AclOperation.IDEMPOTENT_WRITE,
    }

    resource_pattern_type_map = {
        'UNKNOWN': ResourcePatternType.UNKNOWN,
        'ANY': ResourcePatternType.ANY,
        'MATCH': ResourcePatternType.MATCH,
        'LITERAL': ResourcePatternType.LITERAL,
        'PREFIXED': ResourcePatternType.PREFIXED,
    }

    resource_type_map = {
        'UNKNOWN': ResourceType.UNKNOWN,
        'ANY': ResourceType.ANY,
        'TOPIC': ResourceType.TOPIC,
        'GROUP': ResourceType.GROUP,
        'BROKER': ResourceType.BROKER,
    }

    acl_permission_type_map = {
        'UNKNOWN': AclPermissionType.UNKNOWN,
        'ANY': AclPermissionType.ANY,
        'DENY': AclPermissionType.DENY,
        'ALLOW': AclPermissionType.ALLOW,
    }

    acl_operation = operation_map[operation]
    acl_restype = resource_type_map[resource_type]
    acl_resource_pattern = resource_pattern_type_map[resource_pattern]
    acl_permission = acl_permission_type_map[permission]
    acl_binding = AclBinding(
        restype=acl_restype,
        name=resource_name,
        resource_pattern_type=acl_resource_pattern,
        principal=f'User:{username}',
        host='*',
        operation=acl_operation,
        permission_type=acl_permission
    )
    # Create the ACL
    try:
        futures = admin_client.create_acls([acl_binding])
        for acl, future in futures.items():
            future.result()  # The result is None if successful
        return {"message": f"ACL added successfully for {username} on {resource_name}."}
    except Exception as e:
        if e.args[0].code() == KafkaError.CLUSTER_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail=f"Authorization failed. You are not allowed to set ACLs.")
        else:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
@router.delete("/delete-acl/")
async def delete_acl(resource_name: str, username: str, resource_type: str = Query('TOPIC', enum=['ANY', 'TOPIC', 'GROUP', 'BROKER']),  resource_pattern: str = Query('LITERAL', enum=['ANY', 'MATCH', 'LITERAL', 'PREFIXED']) , operation: str = Query('READ', enum=['READ','WRITE','ANY', 'ALL','CREATE','DELETE','ALTER','DESCRIBE','CLUSTER_ACTION', 'ALTER_CONFIGS', 'DESCRIBE_CONFIGS','IDEMPOTENT_WRITE']) , permission: str = Query('ALLOW', enum=['ANY', 'ALLOW', 'DENY']) , user: dict = Depends(get_current_user)): 
    kafka_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanisms': sasl_mechanisms,
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    }
    admin_client = AdminClient(kafka_conf)
    operation_map = {
        'UNKNOWN': AclOperation.UNKNOWN,
        'ANY': AclOperation.ANY,
        'ALL': AclOperation.ALL,
        'READ': AclOperation.READ,
        'WRITE': AclOperation.WRITE,
        'CREATE': AclOperation.CREATE,
        'DELETE': AclOperation.DELETE,
        'ALTER': AclOperation.ALTER,
        'DESCRIBE': AclOperation.DESCRIBE,
        'CLUSTER_ACTION': AclOperation.CLUSTER_ACTION,
        'DESCRIBE_CONFIGS': AclOperation.DESCRIBE_CONFIGS,
        'ALTER_CONFIGS': AclOperation.ALTER_CONFIGS,
        'IDEMPOTENT_WRITE': AclOperation.IDEMPOTENT_WRITE,
    }

    resource_pattern_type_map = {
        'UNKNOWN': ResourcePatternType.UNKNOWN,
        'ANY': ResourcePatternType.ANY,
        'MATCH': ResourcePatternType.MATCH,
        'LITERAL': ResourcePatternType.LITERAL,
        'PREFIXED': ResourcePatternType.PREFIXED,
    }

    resource_type_map = {
        'UNKNOWN': ResourceType.UNKNOWN,
        'ANY': ResourceType.ANY,
        'TOPIC': ResourceType.TOPIC,
        'GROUP': ResourceType.GROUP,
        'BROKER': ResourceType.BROKER,
    }

    acl_permission_type_map = {
        'UNKNOWN': AclPermissionType.UNKNOWN,
        'ANY': AclPermissionType.ANY,
        'DENY': AclPermissionType.DENY,
        'ALLOW': AclPermissionType.ALLOW,
    }
    acl_operation = operation_map[operation]
    acl_restype = resource_type_map[resource_type]
    acl_resource_pattern = resource_pattern_type_map[resource_pattern]
    acl_permission = acl_permission_type_map[permission]
    acl_filter = AclBindingFilter(
        restype=acl_restype,
        name=resource_name,
        resource_pattern_type=acl_resource_pattern,
        principal=f'User:{username}',
        host='*',
        operation=acl_operation,
        permission_type=acl_permission
    )
    # Delete  the ACL
    try:
        futures = admin_client.delete_acls([acl_filter])
        deleted_acls = [future.result() for future in futures.values()]
        return {"message": f"Successfully deleted ACLs for {username} on {resource_name}."}
    except Exception as e:
        if e.args[0].code() == KafkaError.CLUSTER_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail=f"Authorization failed. You are not allowed to set ACLs.")
        else:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/get-user-acl/")
async def get_user_acl(username: str, user: dict = Depends(get_current_user)):
    kafka_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanisms': sasl_mechanisms,
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    }
    admin_client = AdminClient(kafka_conf)
    acl_filter = AclBindingFilter(
        restype=ResourceType.ANY,  # Specify the resource type (e.g., TOPIC)
        name=None,  # Specify the exact name of the topic
        resource_pattern_type=ResourcePatternType.LITERAL,  # Specify the pattern type (e.g., LITERAL for exact name match)
        principal=f'User:{username}',  # Use None to match any principal
        host=None,  # Use None to match any host
        operation=AclOperation.ANY,  # Use None to match any operation
        permission_type=AclPermissionType.ANY  # Use None to match any permission type
    )
    try:
        future = admin_client.describe_acls(acl_filter)
        acl_bindings = future.result()  # This blocks until the future is complete
        transformed_acl_bindings = [acl_binding_to_dict(acl_binding) for acl_binding in acl_bindings]
        return transformed_acl_bindings
    except Exception as e:
       if e.args[0].code() == KafkaError.CLUSTER_AUTHORIZATION_FAILED:
           raise HTTPException(status_code=403, detail=f"Authorization failed. You are not allowed to get the ACLs.")
       else:
           raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/get-topic-acl/")
async def get_topic_acl(topic_name: str, user: dict = Depends(get_current_user)):
    kafka_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanisms': sasl_mechanisms,
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    }
    admin_client = AdminClient(kafka_conf)
    acl_filter = AclBindingFilter(
        restype=ResourceType.TOPIC,  # Specify the resource type (e.g., TOPIC)
        name=topic_name,  # Specify the exact name of the topic
        resource_pattern_type=ResourcePatternType.LITERAL,  # Specify the pattern type (e.g., LITERAL for exact name match)
        principal=None,  # Use None to match any principal
        host=None,  # Use None to match any host
        operation=AclOperation.ANY,  # Use None to match any operation
        permission_type=AclPermissionType.ANY  # Use None to match any permission type
    )
    try:
        future = admin_client.describe_acls(acl_filter)
        acl_bindings = future.result()  # This blocks until the future is complete
        transformed_acl_bindings = [acl_binding_to_dict(acl_binding) for acl_binding in acl_bindings]
        return transformed_acl_bindings
    except Exception as e:
       if e.args[0].code() == KafkaError.CLUSTER_AUTHORIZATION_FAILED: 
           raise HTTPException(status_code=403, detail=f"Authorization failed. You are not allowed to get the ACLs.")
       else: 
           raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
