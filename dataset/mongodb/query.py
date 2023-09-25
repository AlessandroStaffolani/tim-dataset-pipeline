import os
from typing import Optional, Tuple, Dict, Any

from pymongo import MongoClient
from pymongo.cursor import Cursor

from dataset.mongodb.geojson_uploader import get_connection_uri


def get_db():
    client = MongoClient(get_connection_uri())
    to_db = os.getenv('MONGO_DB')
    return client[to_db]


def get_bs_in_range(
        from_collection: str,
        center: Tuple[float, float],
        max_distance: float,
        filters: Optional[Dict[str, Any]] = None,
        db=None
) -> Cursor:
    db = db if db is not None else get_db()
    collection = db[from_collection]
    query = {} if filters is None else filters

    query['geometry'] = {
        '$nearSphere':
            {
                '$geometry': {
                    'type': 'Point', 'coordinates': [center[0], center[1]]
                },
                '$maxDistance': max_distance
            }
    }
    return collection.find(query)
