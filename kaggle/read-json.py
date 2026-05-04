import json

with open("../kaggle/data/arxiv-metadata-oai-snapshot.json", "r") as f:
    first_line = f.readline()

obj = json.loads(first_line)
print(json.dumps(obj, indent=2))