import pandas as pd
import sqlite3
import gzip
import urllib.request
import os
from tqdm import tqdm

class IMDbDatasetLoader:
    def __init__(self, db_path='imdb_real.db'):
        self.db_path = db_path
        self.base_url = 'https://datasets.imdbws.com/'
        self.files = {
            'title_basics': 'title.basics.tsv.gz',
            'title_ratings': 'title.ratings.tsv.gz', 
            'name_basics': 'name.basics.tsv.gz',
            'title_principals': 'title.principals.tsv.gz',
            'title_crew': 'title.crew.tsv.gz',
            'title_akas': 'title.akas.tsv.gz',
            'title_episode': 'title.episode.tsv.gz'
        }
    
    def download_files(self):
        """Download all IMDb dataset files"""
        print("Downloading IMDb dataset files...")
        os.makedirs('imdb_data', exist_ok=True)
        
        for name, filename in self.files.items():
            url = self.base_url + filename
            filepath = f'imdb_data/{filename}'
            
            if not os.path.exists(filepath):
                print(f"Downloading {filename}...")
                urllib.request.urlretrieve(url, filepath)
            else:
                print(f"{filename} already exists, skipping...")
    
    def create_schema(self):
        """Create database schema optimized for IMDb data"""
        conn = sqlite3.connect(self.db_path)
        
        schema_sql = """
        -- Title basics table
        CREATE TABLE IF NOT EXISTS title_basics (
            tconst TEXT PRIMARY KEY,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER,
            genres TEXT
        );
        
        -- Title ratings table  
        CREATE TABLE IF NOT EXISTS title_ratings (
            tconst TEXT PRIMARY KEY,
            averageRating REAL,
            numVotes INTEGER,
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
        );
        
        -- Name basics table
        CREATE TABLE IF NOT EXISTS name_basics (
            nconst TEXT PRIMARY KEY,
            primaryName TEXT,
            birthYear INTEGER,
            deathYear INTEGER,
            primaryProfession TEXT,
            knownForTitles TEXT
        );
        
        -- Title principals table
        CREATE TABLE IF NOT EXISTS title_principals (
            tconst TEXT,
            ordering INTEGER,
            nconst TEXT,
            category TEXT,
            job TEXT,
            characters TEXT,
            PRIMARY KEY (tconst, ordering),
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
            FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
        );
        
        -- Title crew table
        CREATE TABLE IF NOT EXISTS title_crew (
            tconst TEXT PRIMARY KEY,
            directors TEXT,
            writers TEXT,
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_title_basics_type ON title_basics(titleType);
        CREATE INDEX IF NOT EXISTS idx_title_basics_year ON title_basics(startYear);
        CREATE INDEX IF NOT EXISTS idx_title_basics_genres ON title_basics(genres);
        CREATE INDEX IF NOT EXISTS idx_title_ratings_rating ON title_ratings(averageRating);
        CREATE INDEX IF NOT EXISTS idx_title_ratings_votes ON title_ratings(numVotes);
        CREATE INDEX IF NOT EXISTS idx_name_basics_profession ON name_basics(primaryProfession);
        CREATE INDEX IF NOT EXISTS idx_principals_category ON title_principals(category);
        """
        
        conn.executescript(schema_sql)
        conn.close()
        print("Database schema created successfully!")
    
    def load_data_chunk(self, filepath, table_name, chunksize=50000):
        """Load data in chunks to handle large files"""
        conn = sqlite3.connect(self.db_path)
        
        print(f"Loading {table_name}...")
        
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            # Read in chunks to handle large files
            chunk_iter = pd.read_csv(f, sep='\t', chunksize=chunksize, 
                                   na_values=['\\N'], keep_default_na=False)
            
            total_rows = 0
            for chunk in tqdm(chunk_iter, desc=f"Loading {table_name}"):
                # Clean the data
                chunk = chunk.replace('\\N', None)
                
                # Convert numeric columns
                if table_name == 'title_basics':
                    chunk['isAdult'] = pd.to_numeric(chunk['isAdult'], errors='coerce')
                    chunk['startYear'] = pd.to_numeric(chunk['startYear'], errors='coerce')
                    chunk['endYear'] = pd.to_numeric(chunk['endYear'], errors='coerce')
                    chunk['runtimeMinutes'] = pd.to_numeric(chunk['runtimeMinutes'], errors='coerce')
                
                elif table_name == 'title_ratings':
                    chunk['averageRating'] = pd.to_numeric(chunk['averageRating'], errors='coerce')
                    chunk['numVotes'] = pd.to_numeric(chunk['numVotes'], errors='coerce')
                
                elif table_name == 'name_basics':
                    chunk['birthYear'] = pd.to_numeric(chunk['birthYear'], errors='coerce')
                    chunk['deathYear'] = pd.to_numeric(chunk['deathYear'], errors='coerce')
                
                # Load to database
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
                total_rows += len(chunk)
            
            print(f"Loaded {total_rows:,} rows into {table_name}")
        
        conn.close()
    
    def load_all_data(self):
        """Load all IMDb dataset files into database"""
        print("Loading IMDb data into database...")
        
        # Load core tables first
        file_table_mapping = {
            'imdb_data/title.basics.tsv.gz': 'title_basics',
            'imdb_data/title.ratings.tsv.gz': 'title_ratings',
            'imdb_data/name.basics.tsv.gz': 'name_basics',
            'imdb_data/title.principals.tsv.gz': 'title_principals',
            'imdb_data/title.crew.tsv.gz': 'title_crew',
        }
        
        for filepath, table_name in file_table_mapping.items():
            if os.path.exists(filepath):
                self.load_data_chunk(filepath, table_name)
    
    def get_data_summary(self):
        """Get summary of loaded data"""
        conn = sqlite3.connect(self.db_path)
        
        tables = ['title_basics', 'title_ratings', 'name_basics', 'title_principals', 'title_crew']
        
        print("\n📊 Database Summary:")
        print("-" * 50)
        
        for table in tables:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                count = result[0] if result else 0
                print(f"{table}: {count:,} records")
            except:
                print(f"{table}: Table not found")
        
        conn.close()

if __name__ == "__main__":
    loader = IMDbDatasetLoader()
    
    # Download data
    loader.download_files()
    
    # Create database and schema
    loader.create_schema()
    
    # Load data (this will take some time - the dataset is large!)
    loader.load_all_data()
    
    # Show summary
    loader.get_data_summary()
