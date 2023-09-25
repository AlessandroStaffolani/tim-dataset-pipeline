import os
from datetime import datetime
from typing import List, Tuple
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pymongo import MongoClient, GEOSPHERE, TEXT
from pymongo.errors import (PyMongoError, BulkWriteError)

from dataset.utils import ROOT_DIR, load_json_file

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, '.env'))


def get_connection_uri(mongo_password=None):
    host = os.getenv('MONGO_HOST')
    port = os.getenv('MONGO_PORT')
    db = os.getenv('MONGO_DB')
    user = os.getenv('MONGO_USER')
    password = mongo_password if mongo_password is not None else os.getenv('MONGO_PASSWORD')
    replica_set = os.getenv('MONGO_REPLICA_SET')
    url = f'mongodb://{host}:{port}/{db}'
    if user is not None and password is not None:
        if user == 'root':
            url = f'mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}'
        else:
            url = f'mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}'

    if replica_set is not None and len(replica_set) > 0:
        url = f'mongodb://{host}/{db}?replicaSet={replica_set}'
        if user is not None and password is not None:
            if user == 'root':
                url = f'mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}/?replicaSet={replica_set}'
            else:
                url = f'mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}/{db}?replicaSet={replica_set}'

    return url


def upload_geojson(
        geojson_path: str,
        to_collection: str,
        geosphere_index_name: str = 'geometry',
        additional_indexes: List[Tuple[str, str]] = None
):
    geojson = load_json_file(geojson_path)
    client = MongoClient(get_connection_uri())
    to_db = os.getenv('MONGO_DB')
    db = client[to_db]
    collection = db[to_collection]

    # create 2dsphere index and initialize unordered bulk insert
    collection.create_index([(geosphere_index_name, GEOSPHERE)])
    for index in additional_indexes:
        collection.create_index([index])
    bulk = collection.initialize_unordered_bulk_op()

    for feature in geojson['features']:
        # Note: comment out next two lines if input file does not contain timestamp field having proper format
        created = feature['properties']['created']
        feature['properties']['created'] = datetime.fromtimestamp(created)
        updated = feature['properties']['updated']
        feature['properties']['updated'] = datetime.fromtimestamp(updated)

        # append to bulk insert list
        bulk.insert(feature)

    # execute bulk operation to the DB
    try:
        result = bulk.execute()
        print(f"Number of Features successfully inserted: {result['nInserted']}")
    except BulkWriteError as bwe:
        nInserted = bwe.details["nInserted"]
        errMsg = bwe.details["writeErrors"]
        print("Errors encountered inserting features")
        print(f"Number of Features successully inserted: {nInserted}")
        print("The following errors were found:")
        for item in errMsg:
            print(f"Index of feature: {item['index']}")
            print(f"Error code: {item['code']}")
            print(f"Message (truncated due to data length): {item['errmsg'][0:120]}...")
