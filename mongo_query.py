import json
from datetime import datetime, timedelta
from typing import Literal

from pymongo import MongoClient

import config

client = MongoClient(
    host=config.HOST,
    port=config.PORT,
    username=config.USERNAME,
    password=config.PASSWORD,
)[config.DB][config.COLLECTION]


def build_pipeline(
    dt_from: datetime, dt_upto: datetime, group_type: Literal["month", "day", "hour"]
) -> list:
    pipeline_pattern = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {
            "$densify": {
                "field": "dt",
                "range": {"step": 1, "unit": group_type, "bounds": [dt_from, dt_upto]},
            }
        },
        {
            "$group": {
                "_id": {"labels": {"$dateTrunc": {"date": "$dt", "unit": group_type}}},
                "dataset": {"$sum": "$value"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$group": {
                "_id": 0,
                "dataset": {"$push": "$dataset"},
                "labels": {
                    "$push": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%S",
                            "date": "$_id.labels",
                        }
                    }
                },
            }
        },
        {"$project": {"_id": 0}},
    ]
    return pipeline_pattern


def process_query(message: str):
    try:
        msg = json.loads(message)
        assert len(msg) == 3
        assert msg["group_type"] in ["month", "day", "hour"]
        dt_from = datetime.fromisoformat(msg["dt_from"])
        dt_upto = datetime.fromisoformat(msg["dt_upto"]) + timedelta(seconds=1)
        group_type = msg["group_type"]
        pipeline = build_pipeline(dt_from, dt_upto, group_type)
        response_mongo = client.aggregate(pipeline)
        result = json.dumps(list(response_mongo)[0])
        return result
    except Exception:
        return (
            'Невалидный запос. Пример запроса:\n{"dt_from": "2022-09-01T00:00:00", '
            '"dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
        )
