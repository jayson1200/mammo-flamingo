import json
import base64
import io
import tarfile
import random

from  google.cloud import storage

qs_json_path = "../datasets/qs.json"

qs = json.load(open(qs_json_path))

TARGET_EACH_TYPE = 10
BUCKET_NAME = "emory-dataset"

ASSES = "asses"
PATH_SEV = "path_sev"
DENSITY = "density"
VIEW = "view"
AGE = "age"
LOC = "loc"

question_counts = {
    "asses": 0,
    "path_sev": 0,
    "density": 0,
    "view": 0,
    "age": 0,
    "loc": 0
}

total_qs = 0

def download_blob_as_base64(source_blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(source_blob_name)

    blob_data = blob.download_as_bytes()
    base64_str = base64.b64encode(blob_data).decode("utf-8")

    return base64_str


random.shuffle(qs["qs"])

for q in qs["qs"]:
    if q['qtype'] == ASSES or q['qtype'] == PATH_SEV:
        if question_counts[q['qtype']] >= TARGET_EACH_TYPE:
            continue

        for idx, img_path in enumerate(q['img_paths']):
            q['img_paths'][idx] = download_blob_as_base64(img_path)

        question_counts[q['qtype']] += 1
        total_qs += 1
    else:
        if question_counts[q['qtype']] >= TARGET_EACH_TYPE:
            continue

        for idx, img_path in enumerate(q['img_paths']):
            q['img_paths'][idx] = download_blob_as_base64(img_path)

        question_counts[q['qtype']] += 1
        total_qs += 1

 
    if all(ct == TARGET_EACH_TYPE for ct in question_counts.values()):
        break