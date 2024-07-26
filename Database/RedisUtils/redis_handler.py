import redis.asyncio as redis
from typing import Optional, Union, Dict, List
from bson import ObjectId

from settings import settings, Settings

class RedisHandler:
    instance = None
    
    # 싱글톤 객체 생성을 위한 __new__ 오버라이드
    @classmethod
    def __new__(cls, *args, **kwargs):
        if(cls.instacne is None):
            cls.instance = super(RedisHandler, cls).__new__(cls, *args, **kwargs)
        return cls.instance
    
    def __init__(self, redis_settings:Settings=settings, db_setting:Optional[Dict[str,str]]=None)->None:
        if(self.db_conn is None):
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
                self.db_conn = redis.from_url(url, decode_responses=True)
            except Exception as e:
                print(f"RedisHandler Error: {e}")
                return False
        
        if(db_setting is None):
            self.db_schema = None
        else:
            self.db_schema = db_setting.get("db_schema", None)
        
    
    # 연결 해제         
    def close(self):
        if(self.db_conn is not None):
            self.db_conn.close()
    
    # Create => insert
    async def insert(self, documents:Union[Dict[Union[str, ObjectId], str], list[Dict[Union[str, ObjectId], int]]])->bool:
        try:
            # 하나의 객체만 삽입 -> ObjectId 반환
            if(type(documents) is dict):
                # 유효성 검사, 안되면 오류
                if(self.db_schema is not None):
                    temp_data = self.db_schema(**documents)
                    data = temp_data.dict(by_alias=True)
                else:
                    data = documents
                
                key = data.pop("_id")
                value = data
                
                if(not self.db_conn.exists(key)):
                    raise ValueError("Already Existing Value")
                
                result = await self.db_conn.hset(key=key, mapping=value)
                if(result == 1):
                    return True
                else:
                    raise ValueError("Already Existing Value")
            
            # 여러 객체 삽입 -> ObjectId 배열 반환
            elif(type(documents) is list):
                # 유효성 검사, 안되면 오류
                if(self.db_schema is not None):
                    data_list = []
                    for elem in documents:
                        temp_data = self.db_schema(**elem)
                        data = temp_data.dict(by_alias=True)
                        data_list.append(data)
                else:
                    data_list = documents
                 
                # 이미 존재하는 값인지 확인, 있으면 오류 발생   
                for elem in data_list:
                    key = elem.get("_id")
                    if(not self.db_conn.exists(key)):
                        raise ValueError("Already Existing Value")
                
                pipeline = self.db_conn.pipeline()
                for elem in data_list:
                    key = elem.pop("_id")
                    value = elem
                    pipeline.hset(key=key, mapping=value)
                
                result = await pipeline.execute()
                
                result_status = True
                for elem in result:
                    result_status = result_status and elem
                
                if(result_status):
                    return True
                else:
                    raise ValueError("Already Existing Value")                   
            
        # 오류는 False 반환
        except Exception as e:
            print(f"RedisHandler Insert Error: {e}")
            return False
    
    # Read => select
    async def select(self, 
                     keys:Union[Union[str, ObjectId], List[str]],
                     projection:Optional[Dict[str, int]]=None)->Union[Dict, List[Dict], bool]:
        try:
            # key가 문자열이면 1개 검색
            if(type(keys) is Union[str, ObjectId]):
                if(projection is None):
                    result = await self.db_conn.hgetall(keys)
                    # 없으면 오류 반환
                    if(result == {}):
                        raise ValueError("Cannot find data from collection")
                    result = {'_id': keys}.update(result)
                    return result
                else:
                    projection_list = list(projection.keys())
                    result = await self.db_conn.hmget(keys, *projection_list)
                    if(result == None):
                        raise ValueError("Cannot find data from collection")
                    
                    result_dict = {'_id': keys}
                    for i in range(projection_list):
                        result_dict[projection_list[i]] = result[i]                 
                    return result_dict
                    
            # 여러개 반환, dict 배열
            elif(type(keys) is list):
                pipeline = self.db_conn.pipeline()
                if(projection is None):
                    for key in keys:    
                        pipeline.hgetall(key)
                    
                    result = await pipeline.execute()
                    return result
                else:
                    projection_list = list(projection.keys())
                    for key in keys:
                        pipeline.hmget(key, *projection_list)
                    result = await pipeline.execute()
                    result_list = []
                    for idx1 in range(result):
                        result_dict = {'_id', keys[idx1]}
                        for idx2 in range(projection_list):
                            result_dict[projection_list[idx2]] = result[idx1][idx2]
                        result_list.append(result_dict)
                        
                    return result_list
                
        # 오류는 False 반환
        except Exception as e:
            print(f"MongoDBHandler Select Error: {e}")
            return False
        
        
    # update => update
    async def update(self, 
                     filter:Dict={},
                     update:Dict=None)->Union[int, bool]:
        
    # Delete => delete
    async def delete(self, filter:Dict={})->Union[int, bool]:
    
    