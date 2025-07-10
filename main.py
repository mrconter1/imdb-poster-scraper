import requests
import gzip
import shutil
import os
from tqdm import tqdm

def download_title_basics(data_dir='data'):
    os.makedirs(data_dir, exist_ok=True)
    url = "https://datasets.imdbws.com/title.basics.tsv.gz"
    gz_path = os.path.join(data_dir, "title.basics.tsv.gz")
    tsv_path = os.path.join(data_dir, "title.basics.tsv")
    print("Downloading title.basics.tsv.gz...")
    with requests.get(url, stream=True) as r:
        total_size = int(r.headers.get('content-length', 0))
        with open(gz_path, 'wb') as f, tqdm(
            desc="Downloading",
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024
        ) as bar:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
    print("Extracting...")
    with gzip.open(gz_path, 'rb') as f_in, open(tsv_path, 'wb') as f_out:
        chunk_size = 1024 * 1024
        with tqdm(desc="Extracting", unit='B', unit_scale=True, unit_divisor=1024) as bar:
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
                bar.update(len(chunk))
    print("Done.")
    return tsv_path

if __name__ == "__main__":
    download_title_basics() 