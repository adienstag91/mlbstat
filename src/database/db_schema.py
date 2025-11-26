"""
Database Schema Definition
==========================
Defines and creates the PostgreSQL database schema.
"""

import psycopg2
import logging

logger = logging.getLogger(__name__)


def create_schema(conn):
    """
    Create all database tables and indexes if they don't exist.
    
    Args:
        conn: psycopg2 connection object
    """
    cursor = conn.cursor()
    
    try:
        # =====================================================================
        # Players Table
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id VARCHAR(20) PRIMARY KEY,
                full_name VARCHAR(100),
                bats VARCHAR(1),
                throws VARCHAR(1),
                birth_date DATE,
                debut_date DATE,
                height_inches INTEGER,
                weight_lbs INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # =====================================================================
        # Games Table
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS games (
                game_id VARCHAR(50) PRIMARY KEY,
                game_date DATE,
                game_time TIME,
                home_team VARCHAR(50),
                away_team VARCHAR(50),
                runs_home_team INTEGER,
                runs_away_team INTEGER,
                winner VARCHAR(50),
                loser VARCHAR(50),
                venue VARCHAR(200),
                is_playoff BOOLEAN,
                playoff_round VARCHAR(50),
                innings_played INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # =====================================================================
        # Batting Appearances Table
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS batting_appearances (
                id SERIAL PRIMARY KEY,
                game_id VARCHAR(50) REFERENCES games(game_id),
                player_id VARCHAR(20) REFERENCES players(player_id),
                player_name VARCHAR(100),
                team VARCHAR(50),
                batting_order INTEGER,
                positions_played VARCHAR(100),
                is_starter BOOLEAN,
                is_substitute BOOLEAN,
                PA INTEGER DEFAULT 0,
                AB INTEGER DEFAULT 0,
                H INTEGER DEFAULT 0,
                R INTEGER DEFAULT 0,
                RBI INTEGER DEFAULT 0,
                BB INTEGER DEFAULT 0,
                SO INTEGER DEFAULT 0,
                HR INTEGER DEFAULT 0,
                doubles INTEGER DEFAULT 0,
                triples INTEGER DEFAULT 0,
                SB INTEGER DEFAULT 0,
                CS INTEGER DEFAULT 0,
                HBP INTEGER DEFAULT 0,
                GDP INTEGER DEFAULT 0,
                SF INTEGER DEFAULT 0,
                SH INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(game_id, player_id)
            )
        """)
        
        # =====================================================================
        # Pitching Appearances Table
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pitching_appearances (
                id SERIAL PRIMARY KEY,
                game_id VARCHAR(50) REFERENCES games(game_id),
                player_id VARCHAR(20) REFERENCES players(player_id),
                player_name VARCHAR(100),
                team VARCHAR(50),
                is_starter BOOLEAN,
                pitching_order INTEGER,
                decisions VARCHAR(50),
                BF INTEGER DEFAULT 0,
                H_allowed INTEGER DEFAULT 0,
                R_allowed INTEGER DEFAULT 0,
                ER INTEGER DEFAULT 0,
                BB_allowed INTEGER DEFAULT 0,
                SO_pitched INTEGER DEFAULT 0,
                HR_allowed INTEGER DEFAULT 0,
                IP NUMERIC(4,1) DEFAULT 0.0,
                pitches_thrown INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(game_id, player_id)
            )
        """)
        
        # =====================================================================
        # At-Bats (Play-by-Play Events) Table
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS at_bats (
                event_id VARCHAR(100) PRIMARY KEY,
                game_id VARCHAR(50) REFERENCES games(game_id),
                inning INTEGER,
                inning_half VARCHAR(10),
                batter_id VARCHAR(20) REFERENCES players(player_id),
                batter_name VARCHAR(100),
                pitcher_id VARCHAR(20) REFERENCES players(player_id),
                pitcher_name VARCHAR(100),
                description TEXT,
                is_at_bat BOOLEAN,
                is_hit BOOLEAN,
                hit_type VARCHAR(20),
                is_walk BOOLEAN,
                is_strikeout BOOLEAN,
                is_out BOOLEAN,
                outs_recorded INTEGER DEFAULT 0,
                bases_reached INTEGER DEFAULT 0,
                event_order INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # =====================================================================
        # Validation Reports Table
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_reports (
                id SERIAL PRIMARY KEY,
                game_id VARCHAR(50) REFERENCES games(game_id),
                validation_type VARCHAR(20),
                status VARCHAR(20),
                accuracy_percentage NUMERIC(5,2),
                total_official INTEGER,
                total_calculated INTEGER,
                discrepancies_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(game_id, validation_type)
            )
        """)
        
        # =====================================================================
        # Create Indexes for Performance
        # =====================================================================
        
        # Batting appearances indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_batting_game 
            ON batting_appearances(game_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_batting_player 
            ON batting_appearances(player_id)
        """)
        
        # Pitching appearances indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pitching_game 
            ON pitching_appearances(game_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pitching_player 
            ON pitching_appearances(player_id)
        """)
        
        # At-bats indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_atbats_game 
            ON at_bats(game_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_atbats_batter 
            ON at_bats(batter_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_atbats_pitcher 
            ON at_bats(pitcher_id)
        """)
        
        # Validation reports indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_validation_game 
            ON validation_reports(game_id)
        """)
        
        conn.commit()
        logger.info("✅ Database schema created successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create schema: {e}")
        raise e
    finally:
        cursor.close()


def init_database():
    """
    Initialize database by creating schema.
    Convenience function that handles connection.
    """
    from db_connection import get_connection
    
    try:
        conn = get_connection()
        create_schema(conn)
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False


if __name__ == "__main__":
    # Test schema creation
    print("Initializing database schema...")
    success = init_database()
    if success:
        print("✅ Schema initialized successfully")
    else:
        print("❌ Schema initialization failed")
