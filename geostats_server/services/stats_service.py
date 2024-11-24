from sqlalchemy import create_engine, text
from config import Config
from services.geohash_service import get_geohash
import geohash
import json
import os
import re

# Initialize database connection
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, isolation_level="AUTOCOMMIT")

# Global dictionary for stats queries
STATS_QUERIES = {}

def load_stats_queries(stats_folder="stats"):
    """
    Load SQL queries from .sql files in the specified folder.
    The file name (without extension) is used as the stat name.
    Process the SQL to:
    - Remove line comments (`--`).
    - Remove extra whitespaces.
    - Replace line breaks with spaces.
    """
    global STATS_QUERIES
    if not os.path.exists(stats_folder):
        print(f"Stats folder '{stats_folder}' does not exist.")
        return

    STATS_QUERIES = {}
    for file_name in os.listdir(stats_folder):
        if file_name.endswith(".sql"):
            stat_name = os.path.splitext(file_name)[0]
            file_path = os.path.join(stats_folder, file_name)
            with open(file_path, "r", encoding="utf-8") as file:
                raw_query = file.read().strip()
                if len(raw_query) == 0:
                    continue

                # Remove line comments
                query_no_comments = re.sub(r'--.*', '', raw_query)

                # Remove extra whitespaces and replace linebreaks with spaces
                cleaned_query = re.sub(r'\s+', ' ', query_no_comments).strip()

                STATS_QUERIES[stat_name] = cleaned_query

    print(f"Loaded {len(STATS_QUERIES)} stats queries from '{stats_folder}'.")
    create_stats_table()


def create_stats_table():
    """
    Creates the table for storing geohash stats if it doesn't exist.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS geohash_stats (
        geohash VARCHAR(12) PRIMARY KEY,
        center_lat FLOAT NOT NULL,
        center_lng FLOAT NOT NULL,
        stats JSONB NOT NULL
    );
    """
    with engine.connect() as connection:
        connection.execute(text(create_table_query))
        print("Table 'geohash_stats' created or already exists.")


def clear_cached_results():
    """
    Truncates the geohash_stats table, clearing all cached statistics.
    """
    truncate_query = text("TRUNCATE TABLE geohash_stats;")
    with engine.connect() as connection:
        connection.execute(truncate_query)
        print("Table 'geohash_stats' truncated successfully.")
    load_stats_queries()

def query_cached_stats(geohash_code):
    """
    Query the cached stats for a geohash.
    """
    query = text("SELECT stats FROM geohash_stats WHERE geohash = :geohash")
    with engine.connect() as connection:
        result = connection.execute(query, {'geohash': geohash_code}).fetchone()
    return result[0] if result else None


def save_stats(geohash_code, center_lat, center_lng, stats):
    """
    Save calculated stats to the database.
    """
    insert_query = text("""
        INSERT INTO geohash_stats (geohash, center_lat, center_lng, stats)
        VALUES (:geohash, :center_lat, :center_lng, :stats)
        ON CONFLICT (geohash) DO UPDATE
        SET stats = EXCLUDED.stats;
    """)
    with engine.connect() as connection:
        connection.execute(
            insert_query,
            {
                'geohash': geohash_code,
                'center_lat': center_lat,
                'center_lng': center_lng,
                'stats': json.dumps(stats)  # SQLAlchemy automatically handles JSON serialization
            }
        )

def calculate_stat(lat, lng, query):
    """
    Calculate a single statistic using a provided SQL query.
    """
    with engine.connect() as connection:
        result = connection.execute(text(query), {'lat': lat, 'lng': lng}).fetchone()
    return result[0] if result and result[0] is not None else 0


def calculate_stats(lat, lng, geohash_precision=6):
    """
    Calculate statistics for a given latitude and longitude by using the geohash center point.
    If stats already exist in the database, return the cached result.
    """
    # Convert lat/lng to geohash
    geohash_code = get_geohash(lat, lng, geohash_precision)
    calculate_stats_geohash(geohash_code)

def calculate_stats_geohash(geohash_code):
    if len(STATS_QUERIES) == 0:
        load_stats_queries()
    # Check for cached stats
    cached_stats = query_cached_stats(geohash_code)
    if cached_stats:
        return {
            'geohash': geohash_code,
            'stats': cached_stats
        }

    # Decode the geohash to find its center point
    center_lat, center_lng = geohash.decode(geohash_code)

    # Calculate stats using dynamically loaded queries
    stats = {}
    for stat_name, query in STATS_QUERIES.items():
        stats[stat_name] = calculate_stat(center_lat, center_lng, query)

    # Save stats to the database
    save_stats(geohash_code, center_lat, center_lng, stats)

    return {
        'geohash': geohash_code,
        'center_point': {'lat': center_lat, 'lng': center_lng},
        'stats': stats
    }
