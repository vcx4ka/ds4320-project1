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

def create_performance_table():
    """Create the performance table with scores and decline flags"""
    logger.info("Starting performance table creation process...")

    try:
        conn = duckdb.connect('soccer_analytics.duckdb')
        logger.info("Creating performance table with scores and decline flags")
        conn.execute("""
            CREATE OR REPLACE TABLE performance AS
            WITH base_scores AS (
                SELECT
                    CONCAT(pms.player_id, '_', pms.match_id) AS performance_id,
                    pms.player_id,
                    pms.match_id,
                    pms.match_date,
                    pms.rating,
                    pms.finishing,
                    pms.shot_power,
                    pms.dribbling,
                    pms.short_passing,
                    pms.long_passing,
                    pms.acceleration,
                    pms.stamina,
                    -- Performance score using REAL attributes (normalized to 0-100)
                    ((pms.rating * 2) + 
                     (pms.finishing * 1.5) + 
                     (pms.shot_power * 1) + 
                     (pms.dribbling * 1.5) + 
                     (pms.short_passing * 1) + 
                     (pms.long_passing * 1.5) +
                     (pms.acceleration * 1) + 
                     (pms.stamina * 1)) / 1000 * 100 AS performance_score,
                    0 as decline_flag
                FROM player_match_stats pms
            ),
            with_decline AS (
                SELECT
                    bs.*,
                    LAG(bs.performance_score) OVER (
                        PARTITION BY bs.player_id
                        ORDER BY bs.match_date
                    ) AS prev_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY bs.player_id 
                        ORDER BY bs.match_date
                    ) AS games_played
                FROM base_scores bs
            ),
            with_rolling AS (
                SELECT
                    wd.*,
                    AVG(wd.performance_score) OVER (
                        PARTITION BY wd.player_id 
                        ORDER BY wd.match_date 
                        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                    ) AS rolling_avg_3
                FROM with_decline wd
            )
            SELECT
                wr.performance_id,
                wr.player_id,
                wr.match_id,
                wr.match_date,
                wr.rating,
                wr.finishing,
                wr.shot_power,
                wr.dribbling,
                wr.short_passing,
                wr.long_passing,
                wr.acceleration,
                wr.stamina,
                wr.performance_score,
                CASE 
                    WHEN wr.prev_score IS NOT NULL AND wr.performance_score < wr.prev_score
                        THEN 1
                        ELSE 0
                END AS decline_flag,
                COALESCE(wr.rolling_avg_3, wr.performance_score) AS rolling_avg_3,
                wr.games_played,
                pl.height,
                pl.weight,
                EXTRACT(YEAR FROM AGE(wr.match_date, pl.birthday)) AS age
            FROM with_rolling wr
            LEFT JOIN players pl ON wr.player_id = pl.player_id
        """)
        
        # Get row count
        count = conn.execute("SELECT COUNT(*) FROM performance").fetchone()[0]
        logger.info(f"Created performance table with {count:,} rows")

        # Export table
        export_table(conn, "performance")

    except Exception as e:
        logger.error("Error occurred while creating performance table: %s", str(e))
        
    conn.close()
    logger.info("Performance table creation process complete. create_performance.py successfully ran.\n")

if __name__ == "__main__":
    logger.info("RUNNING CREATE_PERFORMANCE_TABLE SCRIPT...")
    create_performance_table()
