"""
Database Connection Management
==============================
Handles PostgreSQL connection setup and management.
"""

import os
import psycopg2
from typing import Optional
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


def get_db_params() -> dict:
    """
    Get database connection parameters from environment variables.
    
    Returns:
        Dict with connection parameters for psycopg2
    """
    return {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'mlb_analytics'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }


def get_connection():
    """
    Create and return a new PostgreSQL database connection.
    
    Returns:
        psycopg2 connection object
        
    Raises:
        Exception if connection fails
    """
    try:
        db_params = get_db_params()
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise e


def check_game_exists(game_url: str) -> bool:
    """
    Check if a game already exists in the database.
    
    Args:
        game_url: Baseball Reference game URL
        
    Returns:
        True if game exists, False otherwise
    """
    from parsing.parsing_utils import extract_game_id
    
    game_id = extract_game_id(game_url)
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT 1 FROM games WHERE game_id = %s",
            (game_id,)
        )
        
        exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        
        return exists
        
    except Exception as e:
        logger.error(f"Error checking if game exists: {e}")
        return False


def check_player_exists(player_id: str) -> bool:
    """
    Check if a player already exists in the database.
    
    Args:
        player_id: Baseball Reference player ID
        
    Returns:
        True if player exists, False otherwise
    """
    if not player_id:
        return False
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT 1 FROM players WHERE player_id = %s",
            (player_id,)
        )
        
        exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        
        return exists
        
    except Exception as e:
        logger.error(f"Error checking if player exists: {e}")
        return False


def test_connection():
    """Test database connection"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        
        print(f"✅ Database connection successful!")
        print(f"PostgreSQL version: {version[0]}")
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


if __name__ == "__main__":
    # Test the connection
    test_connection()
