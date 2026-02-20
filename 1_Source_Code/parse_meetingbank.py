import pandas as pd
from datetime import datetime

# --- PHASE 1: INGESTION ---
# Loading our raw, cleaned dataset from the local file system.
file_path = r"C:\Users\gunar\Downloads\cleaned_file.xlsx"

df = pd.read_excel(file_path)

print("Original Shape:", df.shape)

# --- PHASE 2: FEATURE ENGINEERING ---
# Raw text is difficult to query. We are engineering new numerical and categorical 
# columns dynamically so our Streamlit dashboard has actual metrics to analyze.

# 1. Calculate the exact word count to measure "Meeting Intensity"
df["word_count"] = df["transcript"].astype(str).apply(lambda x: len(x.split()))

# 2. Calculate character length (We use this vs. word count later to prove data quality)
df["transcript_char_length"] = df["transcript"].astype(str).apply(len)

# 3. Extract the city name from the UID (e.g., 'bostoncc' from 'bostoncc_01102018_123')
df["city"] = df["uid"].astype(str).apply(lambda x: x.split("_")[0])

# 4. Custom logic to extract the exact meeting year from the UID string.
# The UID format is typically city_MMDDYYYY_agenda. We slice the date block to get the year.
def extract_year(uid):
    parts = str(uid).split("_")
    # Checking if the date block exists and is exactly 8 characters (MMDDYYYY)
    if len(parts) > 1 and len(parts[1]) == 8:
        return parts[1][-4:]  # Grab the LAST 4 digits (the year)
    return "Unknown"

df["meeting_year"] = df["uid"].apply(extract_year)

# 5. Extract the specific agenda code (the final part of the UID string)
df["agenda_code"] = df["uid"].astype(str).apply(lambda x: x.split("_")[-1])

# 6. Add a processing timestamp for pipeline auditing and tracking
df["processing_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Standardize column naming for our databases
df.rename(columns={"id": "meeting_id"}, inplace=True)

print("Upgraded Shape:", df.shape)

# --- PHASE 3: POLYGLOT PERSISTENCE (DATA SPLITTING) ---
# We are splitting the master dataframe into two distinct tables. 
# This allows us to route the right data to the right database.

# TABLE 1: Relational Metadata (Destined for MySQL)
# This only contains lightweight numbers and categories for lightning-fast dashboard queries.
metadata_df = df[[
    "meeting_id",
    "uid",
    "city",
    "meeting_year",
    "agenda_code",
    "summary",
    "word_count",
    "transcript_char_length",
    "processing_timestamp"
]]

# TABLE 2: Unstructured Text (Destined for MongoDB)
# This isolates the massive, 10,000+ word strings so they don't slow down our SQL database.
transcript_df = df[[
    "meeting_id",
    "uid",
    "transcript",
    "word_count",
    "transcript_char_length"
]].copy()

# Renaming specifically for MongoDB document clarity
transcript_df.rename(columns={
    "transcript": "transcript_text",
    "transcript_char_length": "char_length"
}, inplace=True)

# --- PHASE 4: LOCAL EXPORT ---
# Saving the split files locally before they get picked up by the database loaders.
# CSV format for structured SQL, JSON format for NoSQL document loading.
metadata_output = r"C:\Users\gunar\Downloads\meetings_metadata_FIXED.csv"
mongo_output = r"C:\Users\gunar\Downloads\transcripts_collection_FIXED.json"

metadata_df.to_csv(metadata_output, index=False)
transcript_df.to_json(mongo_output, orient="records", indent=4)

print("Files successfully created and ready for database ingestion:")
print("✔ MySQL Target:", metadata_output)
print("✔ MongoDB Target:", mongo_output)