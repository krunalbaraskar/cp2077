"""
SQLite to Supabase Migration Script
Migrates data from local user.db to Supabase
"""
import sqlite3
import os
from pathlib import Path

# Load .env file
from dotenv import load_dotenv
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

from supabase import create_client

# ============ CONFIGURATION ============
SQLITE_DB_PATH = "data/db/user.db"

# These are loaded from .env
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
    exit(1)

# ============ TABLES TO MIGRATE ============
TABLES = [
    "user_handle",
    "cf_user_cache",
    "duelist",
    "duel",
    "challenge",
    "user_challenge",
    "reminder",
    "rankup",
    "auto_role_update",
    "rated_vcs",
    "rated_vc_users",
    "rated_vc_settings",
    "starboard_config_v1",
    "starboard_emoji_v1",
    "starboard_message_v1",
    "multiplayer_duel",
    "multiplayer_duel_participant",
    "multiplayer_duel_problem",
]

# Column mappings (SQLite column -> Supabase column if different)
COLUMN_MAPPINGS = {
    "cf_user_cache": {"maxRating": "max_rating"},
}


def migrate():
    print(f"Connecting to SQLite database: {SQLITE_DB_PATH}")
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print(f"Connecting to Supabase: {SUPABASE_URL}")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    for table_name in TABLES:
        print(f"\n{'='*50}")
        print(f"Migrating table: {table_name}")
        
        try:
            cursor = conn.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            
            if not rows:
                print(f"  No data in {table_name}, skipping...")
                continue
            
            print(f"  Found {len(rows)} rows")
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Convert to list of dicts
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                
                # Apply column mappings if needed
                if table_name in COLUMN_MAPPINGS:
                    for old_col, new_col in COLUMN_MAPPINGS[table_name].items():
                        if old_col in row_dict:
                            row_dict[new_col] = row_dict.pop(old_col)
                
                # Convert None to null-safe values for primary keys
                data.append(row_dict)
            
            # Insert in batches of 500
            batch_size = 500
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                try:
                    result = supabase.table(table_name).upsert(batch).execute()
                    print(f"  Inserted batch {i//batch_size + 1}: {len(result.data)} rows")
                except Exception as e:
                    print(f"  Error inserting batch: {e}")
                    # Try inserting one by one
                    for j, row in enumerate(batch):
                        try:
                            supabase.table(table_name).upsert(row).execute()
                        except Exception as e2:
                            print(f"    Failed row {i+j}: {e2}")
                            print(f"    Data: {row}")
            
        except sqlite3.OperationalError as e:
            print(f"  Table {table_name} doesn't exist in SQLite: {e}")
        except Exception as e:
            print(f"  Error migrating {table_name}: {e}")
    
    conn.close()
    print(f"\n{'='*50}")
    print("Migration complete!")


if __name__ == "__main__":
    migrate()
