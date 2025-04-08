
from fastapi import FastAPI
from routers import  auth, organization, users, project, project_keys, collections, send_data, get_data, get_data_stats, get_live_data, live_aggregations
from config import settings
app = FastAPI(
    title=settings.app_name,
    version="1.0.1",  # Change version here
    description="The ECO-Ready API provides endpoints for managing the ECO-Ready platform.",
)

#app.include_router(managerial.router)
app.include_router(auth.router, prefix="/api/v1")
app.include_router(organization.router, prefix="/api/v1")
app.include_router(users.router, prefix="/api/v1")
app.include_router(project.router, prefix="/api/v1")
app.include_router(project_keys.router, prefix="/api/v1")
app.include_router(collections.router, prefix="/api/v1")
app.include_router(send_data.router, prefix="/api/v1")
app.include_router(get_data.router, prefix="/api/v1")
app.include_router(get_data_stats.router, prefix="/api/v1")
app.include_router(get_live_data.router, prefix="/api/v1")
app.include_router(live_aggregations.router, prefix="/api/v1")
