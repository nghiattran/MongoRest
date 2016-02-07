import bson
from bson.objectid import ObjectId
from pymongo import MongoClient, ReturnDocument, CursorType

database = 'test_collection'


class MongoRest(object):
    __client__ = None
    __db__ = None
    __database_name__ = None

    def __init__(self, database, ip=None):
        self.__database_name__ = database
        self.__client__ = MongoClient('localhost', ip or 27017)
        self.__db__ = self.__client__[database]
    
    def get(self, collection, params={}, keys=None, skip=0, limit=0):
        table = self.__db__[collection]
        res = table.find(
            filter=params,
            skip=skip,
            limit=limit,
            projection=keys
        )
        return list(res)
    
    def post(self, collection, payload):
        table = self.__db__[collection]
        res = table.insert_one(
            payload
        )
        return payload, res
    
    def post_batch(self, collection, payload, ordered=True,
                   bypass_document_validation=False):
        table = self.__db__[collection]
        try :
            res = table.insert_many(
                payload,
                ordered=ordered,
                bypass_document_validation=bypass_document_validation)
        except Exception as e:
            return {'error': e.message}
    
        return payload, res
    
    def put(self, collection, object_id, payload, keys=None, sort=None):
        params = {
            '_id': ObjectId(object_id)
        }
        table = self.__db__[collection]
        try :
            res = table.find_one_and_update(
                filter=params,
                update=payload,
                projection=keys,
                return_document=ReturnDocument.AFTER,
                sort=sort
            )
    
            if res == None:
                return {'error': 'Object does not exist'}
        except Exception as e:
            return {'error': e.message}
    
        return res
    
    def put_batch(self, collection,params, payload, upsert=False,
                  bypass_document_validation=False):
        table = self.__db__[collection]
        try :
            res = table.update_many(
                filter=params,
                update=payload,
            )
        except Exception as e:
            return {'error': e.message}
        self.parse_result_object(res)
        return self.parse_result_object(res)


    def delete(self, collection, object_id, keys=None, sort=None):
        params = {
            '_id': ObjectId(object_id)
        }
        table = self.__db__[collection]
        try :
            res = table.find_one_and_delete(
                filter=params,
                projection=keys,
                sort=sort
            )
    
            if res == None:
                return {'error': 'Object does not exist'}
        except Exception as e:
            return {'error': e.message}
    
        return res

    def delete_batch(self, collection, params):
        table = self.__db__[collection]
        try :
            res = table.delete_many(
                filter=params,
            )
        except Exception as e:
            return {'error': e.message}

        return self.parse_result_object(res)
    
    def count(self, collection, params, **kwargs):
        table = self.__db__[collection]
        try :
            res = table.count(
                filter=params,
                **kwargs
            )
        except Exception as e:
            return {'error': e.message}
    
        return res

    def parse_result_object(self, object):
        attributes = dir(object)
        list = {}
        for attr in attributes:
            if attr[0] != '_':
                try:
                    list[attr] = object.__getattribute__(attr)
                except Exception:
                    continue
        return list

    def drop_collection(self, collection):
        self.__db__.drop_collection(collection)

    def drop_database(self):
        self.__client__.drop_database(self.__database_name__)