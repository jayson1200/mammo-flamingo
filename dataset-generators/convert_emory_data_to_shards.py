import json
import base64
import io
import tarfile
import random
from tempfile import NamedTemporaryFile

from google.cloud import storage

qs_json_path = "../datasets/qs.json"

qs = json.load(open(qs_json_path))

TARGET_EACH_TYPE = 500
BUCKET_NAME = "emory-dataset"
SHARD_SIZE = 250

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


def dictionaries_to_tar_in_gcp(dictionaries, folder_name, shard_num):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    with NamedTemporaryFile() as temp_tar:
        with tarfile.open(temp_tar.name, "w") as tar:
            for i, data in enumerate(dictionaries):
                with NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as temp_json:
                    json.dump(data, temp_json)
                    temp_json.seek(0)
                    tar.add(temp_json.name, arcname=f"data_{i}.json")
                    temp_json.close() 

        temp_tar.seek(0) 
        blob = bucket.blob(f"{folder_name}/shard-{shard_num}.tar") 
        blob.upload_from_filename(temp_tar.name)


def create_formatted_dataset(qs, start_idx, folder_name):
    curr_shard_qs = []
    num_shards = 0
    last_idx = 0

    for q in qs["qs"][start_idx:]:
        last_idx += 1
        if question_counts[q['qtype']] >= TARGET_EACH_TYPE:
            continue

        for idx, img_path in enumerate(q['img_paths']):
            q['img_paths'][idx] = download_blob_as_base64(img_path)
        
        curr_shard_qs.append(q)

        question_counts[q['qtype']] += 1
        total_qs += 1

        if total_qs % SHARD_SIZE == 0 and total_qs != 0:
            dictionaries_to_tar_in_gcp(curr_shard_qs, folder_name, num_shards)
            curr_shard_qs = []
            num_shards += 1


        if all(ct == TARGET_EACH_TYPE for ct in question_counts.values()):
            break
    
    if len(curr_shard_qs) > 0:
        dictionaries_to_tar_in_gcp(curr_shard_qs, folder_name, num_shards)
    
    return start_idx + last_idx

random.shuffle(qs["qs"])

curr_idx = create_formatted_dataset(qs, 0, "train")
curr_idx = create_formatted_dataset(qs, curr_idx, "val")
create_formatted_dataset(qs, curr_idx, "test")