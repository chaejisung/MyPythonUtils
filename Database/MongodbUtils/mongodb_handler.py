from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional, Union, Dict, List
from bson import ObjectId

from settings import settings, Settings

class MongoDBHandler():
    # 싱글톤 객체로 운용되는 db 연결 -> 포트, 
    instance = None
    db_conn = None
    
    # 싱글톤 객체 생성을 위한 __new__ 오버라이드
    @classmethod
    def __new__(cls, *args, **kwargs):
        if(cls.instacne is None):
            cls.instance = super(MongoDBHandler, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    # 컬렉션 연결용 init
    def __init__(self, mongodb_settings:Settings=settings, coll_config:Optional[Dict[str,str]]=None)->None:
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
            db_name = coll_config.get("db_name", None)
            coll_name = coll_config.get("coll_name", None)
            db_schema = coll_config.get("db_schema", None)
            
            if(db_name is None or coll_name is None or db_schema is None):
                raise

            try:
                self.db_coll = MongoDBHandler.db_conn[db_name][coll_name]
                self.db_schema = db_schema
            except Exception as e:
                print(f"MongoDBHandler Error: {e}")
    
    # 연결 해제         
    def close(self):
        if(MongoDBHandler.db_conn is not None):
            MongoDBHandler.db_conn.close()
            
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
            print(f"MongoDBHandler Insert Error: {e}")
            return False
    
    # Read => select
    async def select(self, 
                     filter:Dict={},
                     projection:Optional[Dict[str, str]]=None)->Union[Dict, List[Dict], bool]:
        try:
            # _id 기준으로 검색 시 1개 반환, dict
            if(filter.get("_id", None) is not None):
                result = await self.db_coll.find_one(filter, projection)
                # 없으면 오류 반환
                if(result is None):
                    raise ValueError("Cannot find data from collection")
            # 여러개 반환, dict 배열
            else:
                cursor = self.db_coll.find(filter, projection)
                result_list = await cursor.to_list()
                await cursor.close()
                return result_list
                
        # 오류는 False 반환
        except Exception as e:
            print(f"MongoDBHandler Select Error: {e}")
            return False
        
    # update => update
    async def update(self, 
                     filter:Dict={},
                     update:Dict=None)->Union[int, bool]:
        try:
            # _id 기준으로 검색 시 1개 수정, dict
            if(filter.get("_id", None) is not None):
                result = await self.db_coll.update_one(filter, update)
            # 여러 개 수정
            else:
                result = await self.db_coll.update_many(filter, update)
            
            updated_count = result.modified_count
            if(updated_count == 0):
                raise ValueError("Cannot find data from collection")
            return updated_count    
        
        # 오류는 False 반환
        except Exception as e:
            print(f"MongoDBHandler Update Error: {e}")
            return False
        
    # Delete => delete
    async def delete(self, filter:Dict={})->Union[int, bool]:
        try:
            # _id 기준으로 검색 시 1개 삭제, dict
            if(filter.get("_id", None) is not None):
                result = await self.db_coll.delete_one(filter)
            else:
                result = await self.db_coll.delete_many(filter)
            
            deleted_count = result.deleted_count
            if(deleted_count == 0):
                raise ValueError("Cannot find data from collection")
            return deleted_count
        
        # 오류는 False 반환
        except Exception as e:
            print(f"MongoDBHandler Delete Error: {e}")
            return False