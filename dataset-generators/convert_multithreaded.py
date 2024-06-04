"""Not Tested"""
import json
import base64
import tarfile
import random
from tqdm import tqdm
from tempfile import NamedTemporaryFile
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
qs_json_path = "../datasets/qs.json"
qs = json.load(open(qs_json_path))

TARGET_EACH_TYPE = 500
BUCKET_NAME = "emory-dataset"
SHARD_SIZE = 250
MAX_WORKERS = 8  # Adjust based on your system

# Tracking question types
question_counts = {
    "asses": 0,
    "path_sev": 0,
    "density": 0,
    "view": 0,
    "age": 0,
    "loc": 0
}

# Initialize Google Cloud Storage client
storage_client = storage.Client()

# Thread-safe progress bar
pbar = tqdm(total=TARGET_EACH_TYPE * 6, desc="Processing files", unit="file")

def download_blob_as_base64(source_blob_name):
    """Downloads a blob from Google Cloud Storage as a base64 string."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(source_blob_name)
    return base64.b64encode(blob.download_as_bytes()).decode("utf-8")

def process_question(q, folder_name, shard_num, curr_shard_qs):
    """Downloads images and adds the question to the current shard."""
    for idx, img_path in enumerate(q['img_paths']):
        path = "images/" + "/".join(img_path.split("/")[5:])
        q['img_paths'][idx] = download_blob_as_base64(path)
    curr_shard_qs.append(q)
    with pbar.get_lock():  
        pbar.update(1)

def dictionaries_to_tar_in_gcp(dictionaries, folder_name, shard_num):
    """Creates a tar archive of dictionaries and uploads it to GCP."""
    bucket = storage_client.bucket(BUCKET_NAME)
    with NamedTemporaryFile() as temp_tar:
        with tarfile.open(temp_tar.name, "w") as tar:
            for i, data in enumerate(dictionaries):
                with NamedTemporaryFile(mode="w+", suffix=".json") as temp_json:
                    json.dump(data, temp_json)
                    temp_json.seek(0)
                    tar.add(temp_json.name, arcname=f"data_{i}.json")
        temp_tar.seek(0)
        blob = bucket.blob(f"{folder_name}/shard-{shard_num}.tar")
        blob.upload_from_filename(temp_tar.name)

def create_formatted_dataset(qs, start_idx, folder_name):
    """Processes questions, downloads images, and creates sharded tar files."""
    curr_shard_qs = []
    num_shards = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for q in qs["qs"][start_idx:]:
            if all(ct == TARGET_EACH_TYPE for ct in question_counts.values()):
                break

            if question_counts[q['qtype']] >= TARGET_EACH_TYPE:
                continue

            question_counts[q['qtype']] += 1

            futures.append(executor.submit(process_question, q, folder_name, num_shards, curr_shard_qs))

            if len(curr_shard_qs) >= SHARD_SIZE:
                dictionaries_to_tar_in_gcp(curr_shard_qs, folder_name, num_shards)
                curr_shard_qs = []
                num_shards += 1

        for _ in as_completed(futures):  
            pass

    if curr_shard_qs:
        dictionaries_to_tar_in_gcp(curr_shard_qs, folder_name, num_shards)

    return start_idx + len(futures) 


# Main execution
random.shuffle(qs["qs"])  # Shuffle questions

curr_idx = create_formatted_dataset(qs, 0, "train")
curr_idx = create_formatted_dataset(qs, curr_idx, "val")
create_formatted_dataset(qs, curr_idx, "test")