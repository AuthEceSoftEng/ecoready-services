
from fastapi import FastAPI
from routers import topics,ksql,pyflink,cassandra,authorization,cassandra_routers, cassandra_writer, projects,send_data
from dependencies import get_current_user

app = FastAPI()

app.include_router(topics.router)
app.include_router(cassandra.router)
app.include_router(authorization.router)
app.include_router(cassandra_routers.router)
#app.include_router(cassandra_writer.router)
#app.include_router(ksql.router)
app.include_router(pyflink.router)
app.include_router(projects.router)
app.include_router(send_data.router)
