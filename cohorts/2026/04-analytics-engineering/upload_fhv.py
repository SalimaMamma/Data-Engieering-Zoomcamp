import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# === CONFIG ===
BUCKET_NAME = "yellow-taxi-salima-2"
CREDENTIALS_FILE = "gcp-key.json"
DOWNLOAD_DIR = "./tmp_fhv"

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
CHUNK_SIZE = 8 * 1024 * 1024

# === INIT CLIENT ===
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === FUNCTIONS ===
def download_file(month):
    url = f"{BASE_URL}{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv.gz")

    try:
        print(f"Downloading {url} ...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists. Proceeding...")
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but is not accessible. Abort.")
        sys.exit(1)

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to gs://{BUCKET_NAME}/{blob_name} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            if verify_gcs_upload(blob_name):
                print(f"Uploaded and verified: gs://{BUCKET_NAME}/{blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed upload attempt {attempt + 1} for {blob_name}: {e}")
        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

# === MAIN ===
if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # Download all months in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    # Upload all successfully downloaded files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    print("All FHV 2019 files processed and uploaded to GCS.")
