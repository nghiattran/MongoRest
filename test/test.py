from copy import deepcopy
import bson
from bson.objectid import ObjectId
from . import BaseTest


class TestClass(BaseTest):
    def test_get(self):
        self.sample_post_batch()
        res = self.db.get(collection=self._TEST_TABLE, params={})

        assert len(res['results']) > 0

    def test_get_one_object(self):
        self.sample_post_batch()
        res = self.db.get(collection=self._TEST_TABLE, params={}, limit=1)

        assert len(res['results']) == 1

    def test_get_with_id(self):
        test_object = self.sample_post()
        res = self.db.get(
            collection=self._TEST_TABLE,
            params={'_id': test_object['results']['_id']},
            limit=1)

        assert len(res['results']) == 1

    def test_get_with_unvalid_id(self):
        res = self.db.get(
            collection=self._TEST_TABLE,
            params={'_id': 'id'},
            limit=1)

        assert len(res['results']) == 0

    def test_get_one_object_with_keys(self):
        res = self.db.get(
            collection=self._TEST_TABLE,
            params=self._TEST_DATA.copy(),
            limit=1,
            keys={'name': True})

        assert len(res['results']) == 1
        assert 'name' in res['results'][0]
        assert 'sex' not in res['results'][0]

    def test_post(self):
        res = self.db.post(
            collection=self._TEST_TABLE,
            payload=self._TEST_DATA.copy())

        assert '_id' in res['results']

    def test_post_batch(self):
        res = self.db.post_batch(
            collection=self._TEST_TABLE,
            payload=deepcopy(self._TEST_BATCH_DATA))

        assert len(res['response']['inserted_ids']) == \
               len(self._TEST_BATCH_DATA)

    def test_put(self):
        test_object = self.sample_post()
        res = self.db.put(
            collection=self._TEST_TABLE,
            payload=self._TEST_SET_PUT.copy(),
            object_id=test_object['results']['_id'])

        assert res['results']['name'] == self._TEST_CHANGED_DATA['name']

    def test_put_with_wrong_id(self):
        res = self.db.put(
                collection=self._TEST_TABLE,
                payload=self._TEST_SET_PUT.copy(),
                object_id=ObjectId()
        )

        assert 'error' in res

    def test_put_with_invalid_id(self):
        try:
            res = self.db.put(
                collection=self._TEST_TABLE,
                payload=self._TEST_SET_PUT.copy(),
                object_id='id')
            assert False
        except bson.errors.InvalidId:
            assert True

    def test_put_batch(self):
        self.sample_post_batch()
        res = self.db.put_batch(
            collection=self._TEST_TABLE,
            params=self._TEST_DATA.copy(),
            payload=self._TEST_SET_PUT.copy())

        # assert res['matched_count'] == res['modified_count']

    def test_delete(self):
        test_object = self.sample_post()
        res = self.db.delete(
            collection=self._TEST_TABLE,
            object_id=test_object['results']['_id'])

        assert '_id' in res['results']

    def test_delete_with_wrong_id(self):
        res = self.db.delete(
            collection=self._TEST_TABLE,
            object_id=ObjectId()
        )

        assert 'error' in res

    def test_delete_with_invalid_id(self):
        try:
            res = self.db.delete(
                collection=self._TEST_TABLE,
                object_id='id')
            assert False
        except bson.errors.InvalidId:
            assert True

    def test_delete_batch(self):
        self.sample_post_batch(payload = deepcopy(self._TEST_BATCH_DATA))
        get_res = self.db.get(
            collection=self._TEST_TABLE,
            params=self._TEST_DATA)

        res = self.db.delete_batch(
            collection=self._TEST_TABLE,
            params=self._TEST_DATA)

        assert res['results']['deleted_count'] == len(get_res['results'])

    def test_count(self):
        self.sample_post_batch()
        res = self.db.count(collection=self._TEST_TABLE, params={})

        assert res >= 1

    @classmethod
    def teardown_class(self):
        print ('\nDone testing! Destroying data in test table.')
        self.db.delete_batch(
            collection=self._TEST_TABLE,
            params={})

        print ('Destroying test collection.')
        self.db.drop_collection(collection=self._TEST_TABLE)

        print ('Destroying test database.')
        self.db.drop_database()