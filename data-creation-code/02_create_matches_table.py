import duckdb
import os
import logging

# configure logging setup
LOG_FILE = "logs/data_creation.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def export_table(conn, table_name):
    """Export the players table as a CSV and as a parquet file."""
    logger.info("Starting players table file export process...")

    try:
        # Export to CSV
        csv_path = f"data/csv/{table_name}.csv"
        conn.execute(f"COPY {table_name} TO '{csv_path}' (HEADER, DELIMITER ',')")
        csv_size = os.path.getsize(csv_path)
        logger.info(f"Exported {table_name} table to CSV: {csv_path} ({csv_size:,} bytes)")

        # Export to Parquet
        parquet_path = f"data/parquet/{table_name}.parquet"
        conn.execute(f"COPY {table_name} TO '{parquet_path}' (FORMAT PARQUET)")
        parquet_size = os.path.getsize(parquet_path)
        logger.info(f"Exported {table_name} table to Parquet: {parquet_path} ({parquet_size:,} bytes)")

    except Exception as e:
        logger.error("Error occurred while exporting players table: %s", str(e))

def create_matches_table():
    """Create the matches table from the raw matches table, matches_raw.csv."""
    logger.info("Starting matches table creation process...")

    try:
        logger.info("Connecting to DuckDB")
        conn = duckdb.connect('soccer_analytics.duckdb')
        logger.info("Create matches table from raw CSV")
        conn.execute("""
                    CREATE OR REPLACE TABLE matches AS
                    SELECT
                        id AS match_id,
                        date,
                        home_team_api_id,
                        away_team_api_id,
                        home_team_goal,
                        away_team_goal,
                    FROM read_csv('data-creation-code/raw/matches_raw.csv', AUTO_DETECT=TRUE)
        """)

        count = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        logger.info(f"Created matches table with {count:,} rows")

        # Export table
        export_table(conn, "matches")

    except Exception as e:
        logger.error("Error occurred while creating matches table: %s", str(e))
        
    conn.close()
    logger.info("Matches table creation process complete. create_matches.py successfully ran.\n")

if __name__ == "__main__":
    logger.info("RUNNING CREATE_MATCHES_TABLE SCRIPT...")
    create_matches_table() 