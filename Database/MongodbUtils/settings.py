from dotenv import load_dotenv

load_dotenv()

from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    MONGODB_HOST: str
    MONGODB_PORT: int
    MONGODB_USER: Optional[str] = None
    MONGODB_PASSWORD: Optional[str] = None
    MONGODB_DB_NAME: Optional[str] = None
    
settings = Settings()
