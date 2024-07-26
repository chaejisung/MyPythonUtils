import redis.asyncio as redis
from typing import Optional, Union, Dict, List
from bson import ObjectId

from settings import settings, Settings

class RedisHandler:
    instance = None
    db_conn = None
    
    # 싱글톤 객체 생성을 위한 __new__ 오버라이드
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
                url = f"redis://{host}:{port}"
            else:
                url = f"redis://:{password}@{host}:{port}"
            
            if(db_setting is None):
                url += "/0"
            else:
                url += f"/{db_setting.db_name}"
            
            #연결 이상하면 오류내기
            try:
                RedisHandler.db_conn = redis.from_url(url, decode_responses=True)
            except Exception as e:
                print(f"RedisHandler Error: {e}")
                return False
    
    # 연결 해제         
    def close(self):
        if(RedisHandler.db_conn is not None):
            RedisHandler.db_conn.close()
    
    # Create => insert
    async def insert(self, documents:Union[Dict[str, str], list[Dict[str, str]]])->Union[ObjectId, List[ObjectId], bool]:
        try:
            # 하나의 객체만 삽입 -> ObjectId 반환
            if(type(documents) is dict):
                # 유효성 검사, 안되면 오류
                temp_data = self.db_schema(**documents)
                data = temp_data.dict(by_alias=True)
                
                result = await self.db_coll.insert_one(data)
                return result.inserted_id
            # 여러 객체 삽입 -> ObjectId 배열 반환
            elif(type(documents) is list):
                # 유효성 검사, 안되면 오류
                data_list = []
                for elem in documents:
                    temp_data = self.db_schema(**elem)
                    data = temp_data.dict(by_alias=True)
                    data_list.append(data)
                
                result = await self.db_coll.insert_many(data_list)
                result_list = [id for id in result.values()]
                return result_list
        # 오류는 False 반환
        except Exception as e:
            print(f"RedisHandler Insert Error: {e}")
            return False
    
    # Read => select
    async def select(self, 
                     filter:Dict={},
                     projection:Optional[Dict[str, str]]=None)->Union[Dict, List[Dict], bool]:
        
        
    # update => update
    async def update(self, 
                     filter:Dict={},
                     update:Dict=None)->Union[int, bool]:
        
    # Delete => delete
    async def delete(self, filter:Dict={})->Union[int, bool]:
    
    