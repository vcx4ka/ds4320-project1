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

def create_player_match_stats_table():
    """Create the player_match_stats table by reshaping wide data"""
    logger.info("Starting player_match_stats table creation process...")

    try:
        logger.info("Connecting to DuckDB")
        conn = duckdb.connect('soccer_analytics.duckdb')

        logger.info("Create player_match_stats table")
        # Create the table using real data from player_attributes
        conn.execute("""
            CREATE OR REPLACE TABLE player_match_stats AS
            WITH match_data AS (
                SELECT * FROM read_csv('data-creation-code/raw/matches_raw.csv', AUTO_DETECT=TRUE)
            ),
            -- Get player attributes
            player_attrs AS (
                SELECT * FROM read_csv('data-creation-code/raw/player_attributes_raw.csv', AUTO_DETECT=TRUE)
            ),
            -- Unpivot match players
            home_players AS (
                SELECT
                    id AS match_id,
                    date,
                    'home' AS position,
                    UNNEST([
                        home_player_1, home_player_2, home_player_3, home_player_4, home_player_5,
                        home_player_6, home_player_7, home_player_8, home_player_9, home_player_10,
                        home_player_11
                    ]) AS player_id
                FROM match_data
            ),
            away_players AS (
                SELECT
                    id AS match_id,
                    date,
                    'away' AS position,
                    UNNEST([
                        away_player_1, away_player_2, away_player_3, away_player_4, away_player_5,
                        away_player_6, away_player_7, away_player_8, away_player_9, away_player_10,
                        away_player_11
                    ]) AS player_id
                FROM match_data
            ),
            all_players AS (
                SELECT * FROM home_players
                UNION ALL
                SELECT * FROM away_players
                WHERE player_id IS NOT NULL
            ),
            -- Get the most recent player attributes before each match
            matched_attributes AS (
                SELECT 
                    ap.match_id,
                    ap.player_id,
                    ap.date AS match_date,
                    ap.position,
                    pa.overall_rating,
                    pa.finishing,
                    pa.shot_power,
                    pa.long_shots,
                    pa.dribbling,
                    pa.short_passing,
                    pa.long_passing,
                    pa.ball_control,
                    pa.acceleration,
                    pa.sprint_speed,
                    pa.stamina,
                    pa.strength,
                    pa.aggression,
                    pa.vision,
                    pa.positioning
                FROM all_players ap
                LEFT JOIN player_attrs pa ON ap.player_id = pa.player_api_id
                    AND pa.date <= ap.date
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY ap.match_id, ap.player_id 
                    ORDER BY pa.date DESC
                ) = 1
            )
            SELECT 
                CONCAT(match_id, '_', player_id) AS id,
                match_id,
                player_id,
                match_date,
                position,
                -- Use REAL player attributes as performance metrics
                COALESCE(overall_rating, 65) AS rating,
                COALESCE(finishing, 50) AS finishing,
                COALESCE(shot_power, 50) AS shot_power,
                COALESCE(dribbling, 50) AS dribbling,
                COALESCE(short_passing, 50) AS short_passing,
                COALESCE(long_passing, 50) AS long_passing,
                COALESCE(acceleration, 50) AS acceleration,
                COALESCE(stamina, 50) AS stamina,
                COALESCE(aggression, 50) AS aggression
            FROM matched_attributes
        """)

        # Get row count
        count = conn.execute("SELECT COUNT(*) FROM player_match_stats").fetchone()[0]
        logger.info(f"Created player_match_stats table with {count:,} rows")
        
        # Export table
        export_table(conn, "player_match_stats")
                
    except Exception as e:
        logger.error(f"Error creating player_match_stats table: {str(e)}", exc_info=True)

    conn.close()
    logger.info("Player_match_stats table creation process complete. create_player_match_stats_table.py successfully ran.\n")

if __name__ == "__main__":
    logger.info("RUNNING CREATE_PLAYER_MATCH_STATS_TABLE SCRIPT...")
    create_player_match_stats_table()

