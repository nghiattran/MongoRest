from pymongo import MongoClient
from src.main import MongoRest
from copy import deepcopy

class BaseTest():
    _DATABASE = 'test_collection'
    _TEST_TABLE = 'table'
    _TEST_DATA = {
        'name': 'test',
        'sex': 'male'}
    _TEST_BATCH_DATA = [
        _TEST_DATA.copy(), _TEST_DATA.copy(), _TEST_DATA.copy()
    ]
    _TEST_CHANGED_DATA = {'name': 'has changed'}
    _TEST_SET_PUT = {'$set': _TEST_CHANGED_DATA.copy()}

    db = MongoRest(_DATABASE)

    def sample_post(self):
        return self.db.post(
            collection=self._TEST_TABLE,
            payload=self._TEST_DATA.copy())

    def sample_post_batch(self, payload=None):
        if payload == None:
            payload = deepcopy(self._TEST_BATCH_DATA)
        res = self.db.post_batch(
            collection=self._TEST_TABLE,
            payload=payload)
        return res