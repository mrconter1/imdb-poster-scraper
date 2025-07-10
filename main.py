import requests
import gzip
import shutil
import os
import sys
import csv
import re
from tqdm import tqdm
from bs4 import BeautifulSoup

def download_title_basics(data_dir='data'):
    os.makedirs(data_dir, exist_ok=True)
    url = "https://datasets.imdbws.com/title.basics.tsv.gz"
    gz_path = os.path.join(data_dir, "title.basics.tsv.gz")
    tsv_path = os.path.join(data_dir, "title.basics.tsv")
    
    # Check if extracted TSV file already exists
    if os.path.exists(tsv_path):
        print(f"TSV file already exists at {tsv_path}")
        return tsv_path
    
    # Check if GZ file already exists
    if os.path.exists(gz_path):
        print(f"GZ file already exists at {gz_path}, skipping download...")
    else:
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

def get_imdb_poster_urls(imdb_url):
    """Extract poster URL with height 1000px from IMDB page"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        resp = requests.get(imdb_url, headers=headers)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        poster_div = soup.find("div", class_=lambda x: x and "ipc-poster__poster-image" in x)
        if not poster_div:
            return None
        img_tag = poster_div.find("img")
        if not img_tag or not img_tag.get("src"):
            return None
        image_url = img_tag["src"]
        # Generate UY1000 URL (height 1000px)
        url_uy1000 = re.sub(r'\._V1_.*?\.jpg', '._V1_UY1000.jpg', image_url)
        return url_uy1000
    except Exception as e:
        return None

def process_imdb_data_and_extract_poster_urls(tsv_path, limit=10):
    """Process the IMDB TSV file and extract poster URLs (1000px height) for movies and TV series"""
    print(f"Processing IMDB data from {tsv_path}...")
    
    count = 0
    successful_extractions = 0
    
    with open(tsv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='\t')
        
        for row in reader:
            if count >= limit:
                break
                
            title_type = row.get('titleType', '')
            tconst = row.get('tconst', '')
            
            # Filter for movies and TV series only
            if title_type in ['movie', 'tvSeries']:
                count += 1
                
                # Construct IMDB URL
                imdb_url = f"https://www.imdb.com/title/{tconst}/"
                
                # Get poster URL
                poster_url = get_imdb_poster_urls(imdb_url)
                if poster_url:
                    successful_extractions += 1
                    print(f"Processing {count} of {limit}: {tconst}, {poster_url}")
                else:
                    print(f"Processing {count} of {limit}: {tconst}, No poster found")
    
    print(f"\nSummary: {successful_extractions}/{count} posters found")

if __name__ == "__main__":
    # Download and extract the IMDB dataset
    tsv_path = download_title_basics()
    
    # Process the data and download posters for the first 10 movies/TV series
    process_imdb_data_and_extract_poster_urls(tsv_path, limit=10) 