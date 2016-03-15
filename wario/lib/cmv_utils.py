import json

def pretty_json(json_data):
    return json.dumps(json_data, indent=4, separators=(',', ': '))