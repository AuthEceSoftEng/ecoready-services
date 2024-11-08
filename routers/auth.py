# routers/auth.py

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from services.auth_service import verify_password, create_access_token
from utilities.user_utils import get_user_by_username
from config import settings  # Import the settings directly from config
from datetime import timedelta
from dependencies import get_current_user_from_jwt, verify_superadmin

router = APIRouter(tags=["Authentication"])

@router.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    # Fetch the user by username
    user = get_user_by_username(form_data.username)
    
    # If user does not exist or password is incorrect
    if not user or not verify_password(form_data.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Use the expiration time from the settings
    access_token_expires = timedelta(minutes=settings.jwt_expiration_minutes)
    
    # Create the JWT access token
    access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)
    
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me")
async def read_users_me(current_user: dict = Depends(get_current_user_from_jwt)):
    # Here, current_user is the user data that was extracted from the JWT
    return {"username": current_user.username, "role": current_user.role}

@router.get("/admin-area")
async def access_admin_area(superadmin: dict = Depends(verify_superadmin)):
    return {"message": f"Welcome to the admin area, {superadmin.username}!"}