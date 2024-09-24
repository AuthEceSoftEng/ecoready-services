
from fastapi import FastAPI
from routers import topics,authorization,projects,send_data,cassandra_queries,managerial

from dependencies import get_current_user

app = FastAPI()

app.include_router(managerial.router)
