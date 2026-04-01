import sqlite3
import pandas as pd
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


def connect():
    # Connect to the sqlite database in the raw folder, return connection object
    logger.info("Starting data extraction process...")
    try:
        raw_dir = "data-creation-code/raw"
        db_path = os.path.join(raw_dir, "database.sqlite")
        logger.info("Connecting to the database at path: %s", db_path)
        conn = sqlite3.connect(db_path)
    except Exception as e:
        logger.error("Error occurred while connecting to the database: %s", str(e))
    
    return conn, raw_dir

def extract(conn):
    # Extract necessary tables from the raw database, return table objects
    try:
        logger.info("Extracting data tables from the database...")
        print("Extracting players table...")
        players = pd.read_sql("SELECT * FROM Player;", conn)
        print("Extracting matches table...")
        matches = pd.read_sql("SELECT * FROM Match;", conn)
        print("Extracting player_attributes table...")
        player_attributes = pd.read_sql("SELECT * FROM Player_Attributes;", conn)
        print("Extracting team table...")
        teams = pd.read_sql("SELECT * FROM Team;", conn)
        print("Extracting league table...")
        leagues = pd.read_sql("SELECT * FROM League;", conn)
    except Exception as e:
        logger.error("Error occurred while extracting data from the database: %s", str(e))
        return None, None, None, None, None

    return players, matches, player_attributes, teams, leagues

def export(raw_dir, players, matches, player_attributes, teams, leagues):
    # Save raw files as CSV before data cleaning and dataset formation
    try:
        logger.info("Exporting extracted data to CSV files...")
        print("Saving players and matches as CSV...")
        players.to_csv(os.path.join(raw_dir, "players_raw.csv"), index=False)
        matches.to_csv(os.path.join(raw_dir, "matches_raw.csv"), index=False)
        player_attributes.to_csv(os.path.join(raw_dir, "player_attributes_raw.csv"), index=False)
        teams.to_csv(os.path.join(raw_dir, "teams_raw.csv"), index=False)
        leagues.to_csv(os.path.join(raw_dir, "leagues_raw.csv"), index=False)
        print("Done exporting data to CSV files.")
        logger.info("Data extraction process complete. extract.py successfully ran.\n")
    except Exception as e:
        logger.error("Error occurred while exporting data: %s", str(e))


if __name__ == "__main__":
    logger.info("RUNNING DATA EXTRACTION SCRIPT...")
    conn, raw_dir = connect()
    players, matches, player_attributes, teams, leagues = extract(conn)
    export(raw_dir, players, matches, player_attributes, teams, leagues)