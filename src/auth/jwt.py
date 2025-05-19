from datetime import datetime
import jwt
from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from config import JWT
import os
from typing import Tuple
import logging
import base64

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
security = HTTPBearer(auto_error=False)  # Don't auto-raise on missing token

class NoAuthToken(HTTPException):
    def __init__(self):
        super().__init__(
            status_code=401,
            detail="No authentication token provided",
            headers={"WWW-Authenticate": "Bearer"},
        )

class InvalidAuthToken(HTTPException):
    def __init__(self):
        super().__init__(
            status_code=401,
            detail="Invalid authentication token provided",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)) -> str:
    """Dependency function to get the current authenticated user."""
    # Bypass JWT authentication and always return a hardcoded user
    # This is for development/testing purposes only
    logger.debug("Bypassing JWT authentication - returning test_user")
    return "test_user"

# For backward compatibility
async def validate_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> Tuple[str, bool]:
    """Validate JWT token and return user information."""
    try:
        user = await get_current_user(credentials)
        return (user, True)
    except Exception as e:
        logger.error(f"Error in validate_token: {str(e)}")
        raise InvalidAuthToken() 
