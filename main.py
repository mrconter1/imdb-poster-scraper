import requests
import gzip
import shutil
import os
import sys
import csv
import re
from tqdm import tqdm
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time

# Global parameters for vote thresholds
MOVIE_VOTE_THRESHOLD = 10000
TV_SERIES_VOTE_THRESHOLD = 1000

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

def download_title_ratings(data_dir='data'):
    os.makedirs(data_dir, exist_ok=True)
    url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    gz_path = os.path.join(data_dir, "title.ratings.tsv.gz")
    tsv_path = os.path.join(data_dir, "title.ratings.tsv")
    
    # Check if extracted TSV file already exists
    if os.path.exists(tsv_path):
        print(f"Ratings TSV file already exists at {tsv_path}")
        return tsv_path
    
    # Check if GZ file already exists
    if os.path.exists(gz_path):
        print(f"Ratings GZ file already exists at {gz_path}, skipping download...")
    else:
        print("Downloading title.ratings.tsv.gz...")
        with requests.get(url, stream=True) as r:
            total_size = int(r.headers.get('content-length', 0))
            with open(gz_path, 'wb') as f, tqdm(
                desc="Downloading ratings",
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
    
    print("Extracting ratings...")
    with gzip.open(gz_path, 'rb') as f_in, open(tsv_path, 'wb') as f_out:
        chunk_size = 1024 * 1024
        with tqdm(desc="Extracting ratings", unit='B', unit_scale=True, unit_divisor=1024) as bar:
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
                bar.update(len(chunk))
    print("Ratings done.")
    return tsv_path

def load_ratings_data(ratings_path):
    """Load ratings data into a dictionary for fast lookup"""
    print("Loading ratings data...")
    ratings = {}
    
    # Count total lines for progress bar
    with open(ratings_path, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file) - 1  # Subtract 1 for header
    
    with open(ratings_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='\t')
        with tqdm(desc="Loading ratings", total=total_lines, unit='rows') as pbar:
            for row in reader:
                tconst = row.get('tconst', '')
                num_votes = row.get('numVotes', '0')
                try:
                    ratings[tconst] = int(num_votes)
                except ValueError:
                    ratings[tconst] = 0
                pbar.update(1)
    
    print(f"Loaded ratings for {len(ratings)} titles")
    return ratings

def get_imdb_poster_urls(imdb_url):
    """Extract poster URL with height 1000px from IMDB page"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        resp = requests.get(imdb_url, headers=headers, timeout=10)
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

def process_single_title(args):
    """Process a single title and return the result"""
    row, index, total = args
    tconst = row.get('tconst', '')
    
    # Construct IMDB URL
    imdb_url = f"https://www.imdb.com/title/{tconst}/"
    
    # Get poster URL
    poster_url = get_imdb_poster_urls(imdb_url)
    
    # Small delay to be respectful to IMDB's servers
    time.sleep(0.1)
    
    return {
        'index': index,
        'tconst': tconst,
        'poster_url': poster_url,
        'total': total
    }

def process_imdb_data_and_extract_poster_urls(tsv_path, ratings_data, limit=None, output_csv='poster_urls.csv'):
    """Process the IMDB TSV file and extract poster URLs (1000px height) for movies and TV series"""
    print(f"Processing IMDB data from {tsv_path}...")
    print(f"Writing results to {output_csv}...")
    
    # Load all movie and TV series rows into memory first
    print("Loading movies and TV series from dataset...")
    movie_tv_rows = []
    
    # Get total number of lines in file for progress bar
    with open(tsv_path, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file) - 1  # Subtract 1 for header
    
    with open(tsv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='\t')
        with tqdm(desc="Loading data", total=total_lines, unit='rows') as pbar:
            for row in reader:
                title_type = row.get('titleType', '')
                tconst = row.get('tconst', '')
                
                if title_type in ['movie', 'tvSeries']:
                    # Get vote count from ratings data
                    vote_count = ratings_data.get(tconst, 0)
                    
                    # Apply vote thresholds
                    if title_type == 'movie' and vote_count > MOVIE_VOTE_THRESHOLD:
                        movie_tv_rows.append(row)
                    elif title_type == 'tvSeries' and vote_count > TV_SERIES_VOTE_THRESHOLD:
                        movie_tv_rows.append(row)
                
                pbar.update(1)
    
    total_to_process = len(movie_tv_rows)
    if limit:
        total_to_process = min(limit, total_to_process)
        movie_tv_rows = movie_tv_rows[:limit]
    
    print(f"Found {len(movie_tv_rows)} movies (>{MOVIE_VOTE_THRESHOLD:,} votes) and TV series (>{TV_SERIES_VOTE_THRESHOLD:,} votes) to process")
    print("Starting concurrent processing (3 threads)...")
    
    successful_extractions = 0
    
    # Open CSV file for writing
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['imdb_id', 'poster_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Prepare arguments for concurrent processing
        args_list = [(row, i+1, total_to_process) for i, row in enumerate(movie_tv_rows)]
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Process with progress bar
            for result in tqdm(executor.map(process_single_title, args_list), 
                             total=len(args_list), 
                             desc="Processing titles", 
                             unit='titles'):
                
                if result['poster_url']:
                    successful_extractions += 1
                    print(f"Processing {result['index']} of {result['total']}: {result['tconst']}, {result['poster_url']}")
                    # Write to CSV
                    writer.writerow({'imdb_id': result['tconst'], 'poster_url': result['poster_url']})
                else:
                    print(f"Processing {result['index']} of {result['total']}: {result['tconst']}, No poster found")
                    # Write to CSV with None for poster_url
                    writer.writerow({'imdb_id': result['tconst'], 'poster_url': 'None'})
                
                # Flush the file to ensure data is written immediately
                csvfile.flush()
    
    print(f"\nSummary: {successful_extractions}/{len(movie_tv_rows)} posters found")
    print(f"Results saved to {output_csv}")

if __name__ == "__main__":
    # Download and extract the IMDB datasets
    tsv_path = download_title_basics()
    ratings_path = download_title_ratings()
    
    # Load ratings data
    ratings_data = load_ratings_data(ratings_path)
    
    # Process the data and extract poster URLs for filtered movies/TV series
    process_imdb_data_and_extract_poster_urls(tsv_path, ratings_data) 