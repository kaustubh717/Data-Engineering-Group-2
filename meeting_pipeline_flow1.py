from prefect import flow, task, get_run_logger, serve
import pandas as pd
import pymysql
from pymongo import MongoClient
from datetime import datetime

# =====================================================================
# GROUP 2: DATA ORCHESTRATORS - MASTER PIPELINE
# This script represents the "Brain" of our architecture. It manages 
# extraction, transformation, polyglot loading, and automated scheduling.
# =====================================================================

# -------------------------------------------------
# CONFIGURATION
# -------------------------------------------------
EXCEL_PATH = r"C:\Users\gunar\Downloads\cleaned_file.xlsx"

# MySQL handles our fast, structured metadata
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root123",
    "database": "meeting_pipeline",
    "port": 3306
}

# MongoDB handles our massive, unstructured text transcripts
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "meeting_pipeline"
MONGO_COLLECTION = "transcripts"

# -------------------------------------------------
# AUTOMATED ALERTING HOOKS
# -------------------------------------------------
# Event-driven architecture: We don't want to stare at a terminal all day.
# These hooks automatically trigger when the pipeline finishes or crashes.
def alert_on_failure(flow, flow_run, state):
    print(f" ALERT: Pipeline '{flow_run.name}' FAILED! Admin notified.")

def alert_on_success(flow, flow_run, state):
    print(f" ALERT: Pipeline '{flow_run.name}' SUCCEEDED! Databases updated.")

# -------------------------------------------------
# TASKS (The Building Blocks)
# -------------------------------------------------

# 1. EXTRACTION
# We engineered 'retries' so if the file is temporarily locked, the system 
# heals itself and tries again in 5 seconds instead of crashing.
@task(name="Ingest Data", retries=2, retry_delay_seconds=5)
def extract_data():
    logger = get_run_logger()
    logger.info("Starting automated ingestion...")
    return pd.read_excel(EXCEL_PATH)

# 2. TRANSFORMATION & FEATURE ENGINEERING
@task(name="Transform & Clean Data")
def transform_data(df):
    logger = get_run_logger()
    logger.info("Transforming data and engineering features...")
    
    # Data Validation: Standardizing headers and dropping corrupted records
    df.columns = df.columns.str.strip().str.lower()
    df = df.dropna(subset=['uid'])
    
    # Feature Engineering: Creating measurable analytics from raw text
    df["word_count"] = df["transcript"].astype(str).apply(lambda x: len(x.split()))
    df["char_length"] = df["transcript"].astype(str).apply(len)
    return df

# 3. POLYGLOT LOAD (RELATIONAL)
@task(name="Load MySQL (Metadata)", retries=1, retry_delay_seconds=5, timeout_seconds=60)
def load_mysql(df):
    logger = get_run_logger()
    connection = None
    try:
        connection = pymysql.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
        # Safely setting up our schema
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS meetings_metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            uid VARCHAR(255), summary TEXT, word_count INT, char_length INT, loaded_at DATETIME
        );
        """)
        
        # IDEMPOTENCY FIX: We empty the table before inserting to prevent the "300 duplicate rows" bug.
        # This guarantees our pipeline is safe to run repeatedly.
        cursor.execute("TRUNCATE TABLE meetings_metadata;")
        
        # Inserting fresh metadata for the dashboard
        insert_query = "INSERT INTO meetings_metadata (uid, summary, word_count, char_length, loaded_at) VALUES (%s, %s, %s, %s, %s)"
        for _, row in df.iterrows():
            cursor.execute(insert_query, (row.get("uid"), row.get("summary"), row.get("word_count"), row.get("char_length"), datetime.now()))
        
        connection.commit()
    except Exception as e:
        logger.error(f"MySQL error: {e}")
        raise e
    finally:
        # Failsafe to always close database connections to prevent memory leaks
        if connection:
            cursor.close()
            connection.close()

# 4. POLYGLOT LOAD (NoSQL)
@task(name="Load MongoDB (Transcripts)", retries=1, retry_delay_seconds=5, timeout_seconds=60)
def load_mongodb(df):
    logger = get_run_logger()
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Transforming Pandas DataFrame rows into NoSQL JSON documents
        documents = [{"uid": r.get("uid"), "summary": r.get("summary"), "transcript": str(r.get("transcript", "")), "word_count": r.get("word_count"), "char_length": r.get("char_length"), "loaded_at": datetime.utcnow()} for _, r in df.iterrows()]
        
        # IDEMPOTENCY FIX for NoSQL: Wiping old records before inserting new ones.
        collection.delete_many({}) 
        collection.insert_many(documents)
    except Exception as e:
        logger.error(f"MongoDB error: {e}")
        raise e
    finally:
        if client:
            client.close()

# 5. FINAL ANALYTICS VERIFICATION
@task(name="Run Analytics Query")
def analytics_summary(df):
    logger = get_run_logger()
    logger.info(f" ANALYTICS: Processed {len(df)} records. Avg words: {round(df['word_count'].mean(), 2)}")

# -------------------------------------------------
# SCRIPTED PIPELINE (THE DAG)
# -------------------------------------------------
# We map our tasks into a Directed Acyclic Graph (DAG) using the @flow decorator.
@flow(
    name="Group 2 Automated ETL Pipeline", 
    description="Fully automated polyglot data pipeline.",
    on_completion=[alert_on_success], 
    on_failure=[alert_on_failure]
)
def meeting_pipeline_flow():
    # Strict execution order:
    raw_data = extract_data()
    clean_data = transform_data(raw_data)
    
    # Concurrent loading into our Polyglot architecture
    mysql_state = load_mysql(clean_data)
    mongo_state = load_mongodb(clean_data)
    
    # DEPENDENCY LOCK: Analytics cannot run until BOTH databases report 100% success.
    analytics_summary(clean_data, wait_for=[mysql_state, mongo_state])

# -------------------------------------------------
# MULTI-SCHEDULE DEPLOYMENTS (Group 2 Specialty)
# -------------------------------------------------
if __name__ == "__main__":
    # Force a local run right now to execute our TRUNCATE commands
    meeting_pipeline_flow() 

    # We fulfill our scheduling requirements by deploying two automated workers:
    # 1. A daily sync using a CRON schedule
    daily_deploy = meeting_pipeline_flow.to_deployment(
        name="Daily-Morning-Sync", cron="0 5 * * *", tags=["production", "cron"], version="2.0"
    )
    
    # 2. A continuous backup running every 12 hours
    interval_deploy = meeting_pipeline_flow.to_deployment(
        name="Bi-Daily-Backup", interval=43200, tags=["backup", "interval"], version="2.0"
    )
    
    # Serve turns our terminal into an active background worker waiting for schedules
    serve(daily_deploy, interval_deploy)