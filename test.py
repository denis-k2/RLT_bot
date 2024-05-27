import json
from pathlib import Path

from mongo_query import process_query

with Path("test.json").open("r") as f:
    test_json = json.load(f)


for i in range(3):
    test_msg = test_json[i][f"input_data_{i}"]
    test_msg_json = json.dumps(test_msg)
    test_response: dict = test_json[i][f"response_{i}"]
    result = process_query(test_msg_json)
    assert result == test_response
    print(f"{i + 1} - success")
