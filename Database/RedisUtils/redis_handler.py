import redis.asyncio as redis
from typing import Optional, Union, Dict, List
from bson import ObjectId

from settings import settings, Settings

class RedisHandler:
    instance = None
    db_conn = None
    
    @classmethod
    def __new__(cls, *args, **kwargs):
        if(cls.instacne is None):
            cls.instance = super(RedisHandler, cls).__new__(cls, *args, **kwargs)
        return cls.instance
    
    def __init__(self, redis_settings:Settings=settings, db_setting:Optional[Dict[str,str]]=None)->None:
        if(RedisHandler.db_conn is None):
            host = redis_settings.REDIS_HOST
            port = redis_settings.REDIS_PORT
            password = redis_settings.REDIS_PASSWORD
            
            if(password is None):
                url = f"redis://@{host}:{port}"
            else:
                url = f"redis://:{password}@{host}:{port}"
            
            if(db_setting is None):
                url += "/0"
            else:
                url += f"/{db_setting.db_name}"
            
            RedisHandler.db_conn = redis.from_url(url, decode_responses=True
        
                               
    )