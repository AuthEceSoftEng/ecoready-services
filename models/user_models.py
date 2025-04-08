from pydantic import BaseModel

class UserRequest(BaseModel):
    username: str
    password: str

class Username(BaseModel):
    username: str