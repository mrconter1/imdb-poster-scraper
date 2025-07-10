# IMDB Poster Scraper

A high-performance Python script that extracts poster URLs from popular movies and TV series using IMDB's official datasets.

## What it does

- Downloads IMDB's `title.basics.tsv.gz` (title information) and `title.ratings.tsv.gz` (ratings/votes)
- Filters content by popularity: **movies with >10k votes** and **TV series with >1k votes**
- Scrapes high-quality poster URLs (1000px height) from IMDB pages
- Uses concurrent processing (3 threads) for efficient data extraction
- Outputs results to `poster_urls.csv`

## Results

The script processed **21,371 popular titles** and successfully found **21,365 poster URLs** (99.97% success rate).

### Output: `poster_urls.csv`

```csv
imdb_id,poster_url
tt0000009,https://m.media-amazon.com/images/M/MV5BMzA4OWYwOTctYmViNy00MmU2LWIzNzgtMzZjMGE2MGFlOTc0XkEyXkFqcGc@._V1_UY1000.jpg
tt0000147,https://m.media-amazon.com/images/M/MV5BMTAwOWQ2MWQtNDc3ZS00ZGI2LTkzZWQtMjhkMzFiOTM2MzMyXkEyXkFqcGc@._V1_UY1000.jpg
tt0000502,None
```

**Columns:**
- `imdb_id`: IMDB title identifier (e.g., tt0000009)
- `poster_url`: Direct URL to 1000px height poster image, or "None" if unavailable

## Usage Examples

### Download posters
```python
import pandas as pd
import requests

df = pd.read_csv('poster_urls.csv')
valid_posters = df[df['poster_url'] != 'None']

for _, row in valid_posters.iterrows():
    response = requests.get(row['poster_url'])
    with open(f"{row['imdb_id']}.jpg", 'wb') as f:
        f.write(response.content)
```

### Filter by title type
```python
# Get IMDB page URLs
df['imdb_url'] = 'https://www.imdb.com/title/' + df['imdb_id'] + '/'
```

### Integration with IMDB datasets
```python
# Merge with title.basics.tsv for full metadata
basics = pd.read_csv('data/title.basics.tsv', sep='\t')
merged = pd.merge(df, basics, left_on='imdb_id', right_on='tconst')
```

## Configuration

Adjust vote thresholds in `main.py`:
```python
MOVIE_VOTE_THRESHOLD = 10000      # Movies with >10k votes
TV_SERIES_VOTE_THRESHOLD = 1000   # TV series with >1k votes
```

## Requirements

```bash
pip install requests tqdm beautifulsoup4
```

## Run

```bash
python main.py
```

---

*Dataset contains posters from 1894 to present day, focusing on popular and well-rated content.* 