import pandas as pd
from pymongo import MongoClient

# --- PHASE 3B: NoSQL DOCUMENT STORAGE (MongoDB) ---
# As part of our Polyglot architecture, we are routing the heavy, unstructured 
# transcript text away from our SQL database and into a NoSQL document database.
# MongoDB is schema-less, meaning it has no row-size limits for our 10,000+ word transcripts.

# -----------------------------------
# 1. Connect to MongoDB (The Storage Layer)
# -----------------------------------
# Establishing a local connection. In a production cloud environment, 
# this URI would point to MongoDB Atlas.
client = MongoClient("mongodb://localhost:27017/")

# Force routing to our specific project database and collection
db = client["meeting_pipeline"]
collection = db["transcripts"]

print("Connected to MongoDB database: meeting_pipeline")

# -----------------------------------
# 2. Extract & Validate (The 'E' in ETL)
# -----------------------------------
file_path = r"C:\Users\gunar\Downloads\cleaned_file.xlsx"
df = pd.read_excel(file_path)

# Data Quality Check: Standardizing headers to lowercase instantly
# This prevents key-mapping errors when we convert these rows to JSON documents later.
df.columns = df.columns.str.strip().str.lower()

print("Detected Columns:", df.columns.tolist())

# -----------------------------------
# 3. Document Transformation (The 'T' in ETL)
# -----------------------------------
documents = []

# Relational databases use 'Rows', but NoSQL uses 'Documents' (JSON objects).
# We are iterating through our dataframe to build a custom dictionary for every meeting.
for _, row in df.iterrows():
    
    # Safely extracting the text. If a transcript is missing, it defaults to a blank string
    # instead of throwing a null error that would crash the pipeline.
    transcript_text = str(row.get("transcript", ""))
    
    # Engineering our NoSQL Document schema
    doc = {
        "uid": str(row.get("uid", "")),
        "summary": str(row.get("summary", "")),
        "transcript": transcript_text,
        
        # Recalculating our engineered features right before insertion
        # to guarantee the metadata exactly matches the text payload being stored.
        "word_count": len(transcript_text.split()),
        "char_length": len(transcript_text)
    }

    documents.append(doc)

# -----------------------------------
# 4. Pipeline Idempotency (The Safety Net)
# -----------------------------------
# THE BUG FIX: Early in testing, running the script twice resulted in 200 records.
# To make this pipeline truly 'Idempotent' (safe to run repeatedly), we execute a 
# delete_many({}) command to wipe the collection clean before inserting fresh data.
collection.delete_many({})
print("Old documents cleared. Pipeline Idempotency maintained.")

# -----------------------------------
# 5. Bulk Load (The 'L' in ETL)
# -----------------------------------
# We use insert_many instead of a standard loop. This is a bulk operation 
# that loads all 100 documents in a single network request, drastically improving speed.
if documents:
    collection.insert_many(documents)
    print("Inserted", len(documents), "documents into meeting_pipeline.transcripts successfully.")
else:
    print("No documents found to insert.")

print("MongoDB insertion completed successfully.")