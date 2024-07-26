from dotenv import load_dotenv

load_dotenv()

from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: Optional[str] = None
    
settings = Settings()
