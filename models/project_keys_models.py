from pydantic import BaseModel, Field, field_validator
from typing import Literal
import uuid
import re

# Base model to include key validation logic
class BaseKeyModel(BaseModel):
    key_value: str

    @field_validator('key_value')
    def check_key_format(cls, value):
        if not re.fullmatch(r'^[a-f0-9]{64}$', value):
            raise ValueError("Invalid key format. Expected a 64-character hexadecimal string.")
        return value

class ProjectKeyCreateRequest(BaseModel):
    key_type: Literal['read', 'write', 'master'] = Field(
        ...,
        description="The type of the key for the project. It can be 'read', 'write', or 'master'.",
        example="read"
    )

class ProjectKeyResponse(BaseModel):
    api_key: str
    project_id: uuid.UUID
    key_type: str
    created_at: str

class DeleteKeyRequest(BaseModel):
    key_value: str

class RegenerateKeyRequest(BaseModel):
    key_value: str