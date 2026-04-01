import duckdb
import os
import logging

# configure logging setup
import logging
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


def create_players_table():
    """Create the players table from the raw players table, players_raw.csv."""
    logger.info("Starting players table creation process...")
    
    try:
        logger.info("Connecting to DuckDB")
        conn = duckdb.connect('soccer_analytics.duckdb')

        logger.info("Create players table from raw CSV")
        conn.execute("""
                    CREATE OR REPLACE TABLE players AS
                    SELECT
                        player_api_id AS player_id,
                    player_name,
                    birthday,
                    CASE
                        WHEN height IS NOT NULL THEN height
                        ELSE 0
                    END AS height,
                    CASE
                        WHEN weight IS NOT NULL THEN weight
                        ELSE 0 
                    END AS weight
                FROM read_csv('data-creation-code/raw/players_raw.csv', AUTO_DETECT=TRUE)
        """)

        count = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        logger.info(f"Created players table with {count} rows")

    except Exception as e:
        logger.error("Error occurred while creating players table: %s", str(e))

    # Export table
    export_table(conn, "players")

    conn.close
    logger.info("Players table creation process complete. create_players.py successfully ran.\n")

    return conn

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

if __name__ == "__main__":
    logger.info("RUNNING CREATE_PLAYERS_TABLE SCRIPT...")
    create_players_table()        