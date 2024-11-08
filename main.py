
from fastapi import FastAPI
from routers import managerial, auth, organization, users, project, project_keys, collections

app = FastAPI()

#app.include_router(managerial.router)
app.include_router(auth.router, prefix="/api/v1")
app.include_router(organization.router, prefix="/api/v1")
app.include_router(users.router, prefix="/api/v1")
app.include_router(project.router, prefix="/api/v1")
app.include_router(project_keys.router, prefix="/api/v1")
app.include_router(collections.router, prefix="/api/v1")