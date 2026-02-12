import json
import os
import boto3
import datetime
import logging
from botocore.exceptions import ClientError

# ==============================================================================
#  GENERAL CONFIGURATION
# ==============================================================================

ROLE_NAME = "CUST_DBA_AUDIT"
TARGET_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# --- MAIN REPOSITORY CONFIGURATION (AURORA POSTGRESQL) ---
CENTRAL_REPO_SECRET_NAME = "xxx/yyy/zzzzzzzzzzzz/wwwwwwwww" 
CENTRAL_REPO_HOST = "xxxxxxxxx.yyyyyyyyyy.us-east-1.rds.amazonaws.com"
CENTRAL_REPO_DB = "AUDIT-TEST"
# Table where the data will be consolidated
CENTRAL_TABLE_NAME = "TB_Consolidated_Audit" 

# Database main name
SOURCE_DB_NAME = "dba" 

TARGET_ACCOUNTS = [
    #{"Id": "xxxxxxxxxxx", "Name": "AWS Account X"},
    {"Id": "yyyyyyyyyyy", "Name": "AWS Account Y"},
    #{"Id": "zzzzzzzzzzz", "Name": "AWS Account Z"},
]

# Ignore list for RDS names that not be scanned (Blacklist)
BLACKLIST_RDS = [
    "db-1",
    "db-2",
    "db-3",
]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ==============================================================================
#  IMPORTS AND DRIVERS
# ==============================================================================
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    logger.warning("psycopg2 library not found.")

try:
    import pyodbc
    HAS_MSSQL = True
except ImportError:
    HAS_MSSQL = False
    logger.warning("pyodbc library not found.")

# ==============================================================================
#  AUX FUNCTIONS (AWS / STS / SECRETS)
# ==============================================================================

def assume_role(account_id, role_name):
    sts_client = boto3.client("sts")
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    try:
        resp = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=f"SyncDB-{account_id}")
        creds = resp["Credentials"]
        return boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=TARGET_REGION
        )
    except Exception as e:
        logger.error(f"[ERROR] Assume Role failed on {account_id}: {e}")
        return None

def get_secret_local(local_client, secret_name):
    try:
        resp = local_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in resp:
            return json.loads(resp['SecretString'])
    except ClientError as e:
        logger.warning(f"Secret not found: {secret_name}. Erro: {e}")
    return None

def json_serial(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if str(type(obj)) == "<class 'decimal.Decimal'>":
        return float(obj)
    return str(obj)

# ==============================================================================
#  MAIN REPOSITORY LOGIC (ETL - AURORA POSTGRES)
# ==============================================================================

def get_central_connection(creds):
    """Connecting to the main repository (Aurora PostgreSQL)"""
    if not HAS_POSTGRES: raise Exception("Driver PostgreSQL ausente para o Central")
    
    return psycopg2.connect(
        host=CENTRAL_REPO_HOST,
        user=creds.get('username') or creds.get('user'),
        password=creds.get('password'),
        dbname=CENTRAL_REPO_DB,
        port=5432, 
        connect_timeout=10
    )

def truncate_central_table(conn):
    """
    Cleanup the table on main repository before load data.
    """
    logger.info(f"Cleaning up table: {CENTRAL_TABLE_NAME}...")
    try:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {CENTRAL_TABLE_NAME}")
        conn.commit()
        logger.info("Table cleaned up.")
    except Exception as e:
        logger.error(f"Error on cleanup table: {e}")
        conn.rollback()
        raise e

def save_batch_to_central(conn, data_rows, source_account, source_db):
    if not data_rows:
        return 0

    insert_sql = f"""
        INSERT INTO {CENTRAL_TABLE_NAME} 
        (SourceAccount, SourceDB, ID, DESCRIPTION, STATUS, UDATE, INSTALLDATE) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor = conn.cursor()
    count = 0
    try:
        params = []
        for row in data_rows:
            row_upper = {k.upper(): v for k, v in row.items()}
            
            params.append((
                source_account,
                source_db,
                row_upper.get('ID'), 
                row_upper.get('DESCRIPTION'), 
                row_upper.get('STATUS'),
                row_upper.get('UDATE'),
                row_upper.get('INSTALLDATE')
            ))
        
        cursor.executemany(insert_sql, params)
        conn.commit()
        count = len(params)
        logger.info(f" -> Inserted {count} records in the main repository {source_db}")
    except Exception as e:
        logger.error(f"Error on saving on main repository: {e}")
        conn.rollback()
        raise e
    
    return count

# ==============================================================================
#  READER FUNCTIONS (SOURCES - FULL LOAD)
# ==============================================================================

def execute_mssql_full(creds, host, db_name, query):
    if not HAS_MSSQL: return []
    
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={host},1433;"
        f"DATABASE={db_name};"
        f"UID={creds.get('username') or creds.get('user')};"
        f"PWD={creds.get('password')};"
        "TrustServerCertificate=yes;Connection Timeout=5;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        return results
    except Exception as e:
        logger.error(f"Error MSSQL Source: {e}")
        return []

def execute_postgres_full(creds, host, db_name, query):
    if not HAS_POSTGRES: return []
    
    try:
        conn = psycopg2.connect(
            host=host,
            user=creds.get('username') or creds.get('user'),
            password=creds.get('password'),
            dbname=db_name,
            port=5432,
            connect_timeout=5
        )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            results = [dict(row) for row in cur.fetchall()]
        conn.close()
        return results
    except Exception as e:
        logger.error(f"Error Postgres Source: {e}")
        return []

# ==============================================================================
#  MAIN HANDLER
# ==============================================================================

def lambda_handler(event, context):
    logger.info("--- STARTING LOAD DATA (FULL LOAD) ---")
    
    local_secrets_client = boto3.client('secretsmanager', region_name=TARGET_REGION)
    
    # 1. Connecting to the main repository (Aurora)
    central_creds = get_secret_local(local_secrets_client, CENTRAL_REPO_SECRET_NAME)
    if not central_creds:
        return {'statusCode': 500, 'body': "Error: No main repository credentials found."}
    
    try:
        central_conn = get_central_connection(central_creds)
        
        # 2. TABLE CLEANUP (FULL LOAD REQUIREMENT)
        truncate_central_table(central_conn)
        
    except Exception as e:
        return {'statusCode': 500, 'body': f"Fatal error on connecting/cleaning up: {e}"}

    total_synced = 0
    report = []

    for account in TARGET_ACCOUNTS:
        acc_id = account['Id']
        acc_name = account['Name']
        
        target_session = assume_role(acc_id, ROLE_NAME)
        if not target_session: continue 
            
        rds_client = target_session.client('rds')
        
        try:
            dbs = rds_client.describe_db_instances()
        except Exception as e:
            logger.error(f"Error listing RDS in the account {acc_id}: {e}")
            continue

        for db in dbs['DBInstances']:
            db_id = db['DBInstanceIdentifier']
            engine = db['Engine']
            host = db['Endpoint']['Address'] if 'Endpoint' in db else None
            
            if db_id in BLACKLIST_RDS or db['DBInstanceStatus'] != 'available':
                continue

            logger.info(f" Loading data from: {db_id} ({engine})")
            secret_name = f"xxx/yyy/{acc_id}/{db_id}"
            creds = get_secret_local(local_secrets_client, secret_name)
            
            if not creds:
                logger.warning(f" [SKIP] No secret: {secret_name}")
                continue

            full_data = []
            
            query_all = "SELECT * FROM TEST_AUDIT" 
            
            if "sqlserver" in engine:
                full_data = execute_mssql_full(creds, host, SOURCE_DB_NAME, query_all)
                
            elif "postgres" in engine:
                full_data = execute_postgres_full(creds, host, SOURCE_DB_NAME, query_all)

            if full_data:
                logger.info(f"   -> Found {len(full_data)} records.")
                inserted = save_batch_to_central(central_conn, full_data, acc_name, db_id)
                total_synced += inserted
                report.append(f"{db_id}: {inserted} records loaded")
            else:
                logger.info("   -> No data found.")

    central_conn.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps({"status": "Success (Full Load)", "total_records": total_synced, "details": report})
    }
