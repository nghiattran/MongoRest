from src.main import MongoRest
from copy import deepcopy
import bson
from bson.objectid import ObjectId

DATABASE = 'test_collection'
TEST_TABLE = 'table'
SAMPLE_DATA = {
    'name': 'test',
    'sex': 'male'}
SAMPLE_BULBLE_DATA = [
    SAMPLE_DATA.copy(), SAMPLE_DATA.copy(), SAMPLE_DATA.copy()
]
SAMPLE_BATCH_DATA = [{
        'name': 'test',
        'sex': 'male'
    },{
        'name': 'test',
        'sex': 'female'}]
SAMPLE_CHANGED_DATA = {'name': 'has changed'}
SAMPLE_SET_PUT = {'$set': SAMPLE_CHANGED_DATA.copy()}
db = MongoRest(DATABASE)
def sample_post():
    return db.post(collection=TEST_TABLE, payload=SAMPLE_DATA.copy())

def sample_post_batch(payload=None):
    if payload == None:
        payload = deepcopy(SAMPLE_BATCH_DATA)
    res = db.post_batch(
        collection=TEST_TABLE,
        payload=payload)
    return res

def test_get():
    res = db.get(collection=TEST_TABLE, params={})

    assert len(res) > 0

def test_get_one_object():
    res = db.get(collection=TEST_TABLE, params={}, limit=1)

    assert len(res) == 1

def test_get_with_id():
    object = sample_post()
    res = db.get(
        collection=TEST_TABLE,
        params={'_id': object[0]['_id']},
        limit=1)

    assert len(res) == 1

def test_get_with_unvalid_id():
    res = db.get(
        collection=TEST_TABLE,
        params={'_id': 'id'},
        limit=1)

    assert len(res) == 0

def test_get_one_object_with_keys():
    res = db.get(
        collection=TEST_TABLE,
        params=SAMPLE_DATA.copy(),
        limit=1,
        keys={'name': True})

    assert len(res) == 1
    assert 'name' in res[0]
    assert 'sex' not in res[0]

def test_post():
    res = db.post(collection=TEST_TABLE, payload=SAMPLE_DATA.copy())

    assert '_id' in res[0]

def test_post_batch():
    res = db.post_batch(
        collection=TEST_TABLE,
        payload=deepcopy(SAMPLE_BATCH_DATA))
    assert len(res[1].inserted_ids) == len(SAMPLE_BATCH_DATA)

def test_put():
    object = sample_post()
    res = db.put(
        collection=TEST_TABLE,
        payload=SAMPLE_SET_PUT.copy(),
        object_id=object[0]['_id'])

    assert res['name'] == SAMPLE_CHANGED_DATA['name']

def test_put__with_wrong_id():
    try:
        res = db.put(
            collection=TEST_TABLE,
            payload=SAMPLE_SET_PUT.copy(),
            object_id='id')
        assert False
    except bson.errors.InvalidId:
        assert True

def test_put_batch():
    sample_post_batch()
    res = db.put_batch(
        collection=TEST_TABLE,
        params=SAMPLE_DATA.copy(),
        payload=SAMPLE_SET_PUT.copy())
    # assert res['matched_count'] == res['modified_count']

def test_delete():
    object = sample_post()
    res = db.delete(
        collection=TEST_TABLE,
        object_id=object[0]['_id'])

    assert '_id' in res

def test_delete():
    sample_post_batch(payload = deepcopy(SAMPLE_BULBLE_DATA))
    get_res = db.get(
        collection=TEST_TABLE,
        params=SAMPLE_DATA)

    res = db.delete_batch(
        collection=TEST_TABLE,
        params=SAMPLE_DATA)

    assert res['deleted_count'] == len(get_res)

def test_count():
    sample_post()
    res = db.count(collection=TEST_TABLE, params={})

    assert res > 1