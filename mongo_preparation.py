from pathlib import Path

from bson import decode_all

from mongo_query import client

with Path("./dump/sampleDB/sample_collection.bson").open("rb") as f:
    data = decode_all(f.read())

client.insert_many(data)
