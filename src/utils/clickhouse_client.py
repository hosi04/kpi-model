import os
from dotenv import load_dotenv
from clickhouse_connect import get_client as ch_get_client

def get_client():
    print("-----Test 005-----")

    # Load environment variables from .env file
    print("Debug 000: Starting to load .env file")
    env_path = '/opt/airflow/.env'
    print(f"Debug 001: Looking for .env file at {env_path}")
    print(f"Debug 002: File exists: {os.path.exists(env_path)}")

    if os.path.exists(env_path):
        print("Debug 003: Loading .env file")
        load_dotenv(env_path)
        print("Debug 004: .env file loaded")
    else:
        print("Debug 005: .env file not found, trying current directory")
        load_dotenv()
        print("Debug 006: Tried loading from current directory")
    
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DATABASE", "default")

    print(f"CLICKHOUSE_HOST: {host}")
    print(f"CLICKHOUSE_PORT: {port}")
    print(f"CLICKHOUSE_USER: {user}")
    print(f"CLICKHOUSE_PASSWORD: {password}")
    print(f"CLICKHOUSE_DATABASE: {database}")
    
    try:
        client = ch_get_client(
            host=host, 
            port=port, 
            username=user, 
            password=password, 
            database=database,
            secure=False  # Use HTTP instead of HTTPS
        )
        print("ClickHouse client created successfully")
        return client
    except Exception as e:
        print(f"Error creating ClickHouse client: {e}")
        raise

def run_sql(sql: str):
    client = get_client()
    client.command(sql)

def run_sql_file(path: str):
    print(f"Running SQL file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    print(f"SQL content: {sql}")
    
    client = get_client()
    
    # Better SQL parsing - remove comments and split properly
    lines = sql.split('\n')
    clean_lines = []
    
    for line in lines:
        # Remove single-line comments
        if '--' in line:
            line = line.split('--')[0]
        clean_lines.append(line)
    
    # Rejoin and split by semicolon
    clean_sql = '\n'.join(clean_lines)
    statements = [s.strip() for s in clean_sql.split(';') if s.strip()]
    
    print(f"Executing {len(statements)} statements")
    
    for i, stmt in enumerate(statements):
        print(f"Executing statement {i+1}: {stmt[:100]}...")
        try:
            client.command(stmt)
            print(f"Statement {i+1} executed successfully")
        except Exception as e:
            print(f"Error executing statement {i+1}: {e}")
            raise
