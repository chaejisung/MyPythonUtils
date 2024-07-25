from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
import asyncio

from settings import settings, Settings

class MongoDBHandler():
    # 싱글톤 객체로 운용되는 db 연결 -> 포트, 
    instance = None
    db_conn = None
    
    # 싱글톤 객체 생성을 위한 __new__ 오버라이드
    def __new__(cls, *args, **kwargs):
        if(cls.instacne is None):
            cls.instance = super(MongoDBHandler, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, mongodb_settings:Settings=settings, coll_config:Optional[dict]=None)->None:
        if(MongoDBHandler.db_conn is None):    
            host = mongodb_settings.MONGODB_HOST
            port = mongodb_settings.MONGODB_PORT
            user = mongodb_settings.MONGODB_USER
            password = mongodb_settings.MONGODB_PASSWORD
            
            if(user is None or password is None):
                url = f'mongodb://{host}:{port}'
            else:
                url = f'mongodb://{user}:{password}@{host}:{port}'
                
            MongoDBHandler.db_conn = AsyncIOMotorClient(url)
        
        if(coll_config is not None):
            db_name = mongodb_settings.MONGODB_DB_NAME
            coll_name = coll_config["coll_name"]

            try:
                self.db_coll = MongoDBHandler.db_conn[db_name][coll_name]
            except Exception as e:
                print(f"MongoDBHandler Errer: {e}")
                
    def close(self):
        if(MongoDBHandler.db_conn is not None):
            MongoDBHandler.db_conn.close()
