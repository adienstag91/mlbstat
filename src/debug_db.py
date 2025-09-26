# debug_db.py
import sqlite3

def test_basic_db_creation():
    print("Testing basic database creation...")
    
    try:
        # Test 1: Basic connection
        with sqlite3.connect("debug_test.db", timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)")
            conn.commit()
            print("✅ Basic database creation works")
            
        # Test 2: Your schema
        print("Testing your schema...")
        with sqlite3.connect("debug_test2.db", timeout=30.0) as conn:
            cursor = conn.cursor()
            # Try just one table first
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    player_id VARCHAR(100) PRIMARY KEY,
                    full_name VARCHAR(200) NOT NULL
                );
            """)
            conn.commit()
            print("✅ Single table creation works")
            
    except Exception as e:
        print(f"❌ Database creation failed: {e}")

if __name__ == "__main__":
    test_basic_db_creation()