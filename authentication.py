from fastapi import HTTPException, status, Depends
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv
import os

load_dotenv()
api_key_header = APIKeyHeader(name='X-API-Key')
api_keys = [os.environ.get('API_KEY')]
api_keys_admin = [os.environ.get('API_KEY_ADMIN')]

def get_api_key(api_key: str = Depends(api_key_header)) -> str:
    if api_key in api_keys:
        return api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Invalid or missing API Key',
    )

def get_api_key_admin(api_key: str = Depends(api_key_header)) -> str:
    if api_key in api_keys_admin:
        return api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Invalid or missing API Key',
    )