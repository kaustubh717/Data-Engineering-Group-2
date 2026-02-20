# MeetingBank Automated ETL & Analytics Platform  
Group 2 – Data Orchestrators
Group Members: Guna Prakash, Kaustubh Z, Niranjan V, Manav C.


This project presents an end-to-end automated data engineering pipeline built using Prefect, Python, MySQL, MongoDB, and Streamlit. The goal of the system is to ingest municipal meeting transcript data, transform and store it efficiently using a polyglot database architecture, and provide meaningful analytics through an interactive dashboard.

The project demonstrates workflow orchestration, automated scheduling, monitoring, feature engineering, and real-time visualization in a production-style ETL setup.

---

## Requirements

Install the following before running the project:

- Python 3.9+
- MySQL Server (running locally)
- MongoDB Community Edition (running locally)

Install Python dependencies: pip install prefect pandas sqlalchemy pymysql pymongo streamlit

## System Architecture

![System Architecture](architecture_diagram.png)

The platform follows a five-layer architecture designed to separate responsibilities clearly and ensure scalability.

### Orchestration Layer
Prefect is used to orchestrate the ETL workflow. It handles scheduling, retries, logging, and automated alerts. This ensures the pipeline runs reliably and can recover from temporary failures.

### Data Ingestion and Processing
Data ingestion is performed using Python and Pandas. The system extracts meeting data from structured files, performs cleaning and validation, and engineers additional analytical features such as transcript length and metadata attributes.

### Storage Layer (Polyglot Persistence)
The project uses two databases intentionally:

- MySQL stores structured metadata for fast querying and analytics.
- MongoDB stores full meeting transcripts, which can be large and unstructured.

This approach improves performance while maintaining flexibility for large text data.

### Monitoring and Alerting
Prefect logging and event tracking provide visibility into pipeline execution. This allows quick identification of failures, completion status, and operational metrics.

### Analytics and Visualization
The processed data is visualized through a Streamlit dashboard. SQLAlchemy is used for querying structured data efficiently.

---

## Key Features

### Automated ETL Pipeline
The pipeline is fully automated and orchestrated through Prefect. It includes retry logic, timeout protection, and idempotent design to prevent duplicate data loads.

### Polyglot Storage Strategy
Separating structured metadata from unstructured transcripts improves performance, scalability, and system maintainability.

### Feature Engineering
Several analytical features are generated automatically, including:

- Word counts
- Character lengths
- City extraction from identifiers
- Additional metadata enrichment

### Analytics Dashboard
The Streamlit dashboard provides:

- Pipeline success metrics
- Meeting distribution by city
- Transcript content analysis
- Data quality validation

### Automated Scheduling
The pipeline supports scheduled executions through CRON jobs and interval-based deployments for continuous data updates.

---

## Technology Stack

Programming and Libraries:
- Python
- Pandas
- SQLAlchemy
- PyMySQL
- PyMongo

Data Infrastructure:
- Prefect for workflow orchestration
- MySQL for relational data storage
- MongoDB for document storage

Visualization:
- Streamlit dashboard

---

## Repository Structure
MeetingBank-ETL/
│
├── architecture_diagram.png
│
├── app.py
├── meeting_pipeline_flow1.py
├── parse_meetingbank.py
├── insert_mongodb.py
│
├── outputs/
│ ├── BiDaily_Backup_ETL_Deployment_Details.jpeg
│ ├── ETL_Flow_Run_Completion_Event.jpeg
│ ├── ETL_Pipeline_Event_Feed_Monitoring.jpeg
│ ├── ETL_Project_Run_Log_Evidence.jpeg
│ ├── ETL_Workflow_Run_Status_Dashboard.jpeg
│ ├── MeetingBank_Analytics_Dashboard_Overview.jpeg
│ ├── MeetingBank_Content_Depth_Analytics_Per_City.jpeg
│ ├── MeetingBank_Data_Quality_Text_Length_Analysis.jpeg
│ ├── MeetingBank_ETL_Deployments_Dashboard.jpeg
│ ├── MeetingBank_ETL_Flows_Overview.jpeg
│ ├── MeetingBank_ETL_Pipeline_Dashboard_Overview.jpeg
│ ├── MeetingBank_ETL_Run_Execution_Overview.jpeg
│ ├── MeetingBank_ETL_Run_History_Overview.jpeg
│ ├── MeetingBank_Meetings_Per_City_Analytics.jpeg
│ └── MeetingBank_MySQL_ETL_Data_Load_View.jpeg
│
├── meetings_metadata_FIXED.csv
└── README.md


## Running the Project

### Start Required Services
Make sure the following services are running locally:

- MySQL (port 3306)
- MongoDB

### Run the ETL Pipeline
python meeting_pipeline_flow1.py


This will execute the pipeline, populate both databases, and register scheduled deployments.

### Launch the Dashboard
streamlit run app.py

---

## Example Analytics Results

From a sample run:

- Over 100 meetings processed
- Multiple municipalities analyzed
- Average transcript length around 2,000 words
- Longest transcript exceeding 6,000 words

These results confirm successful ingestion, transformation, and storage.

---

## Engineering Practices Applied

- Idempotent pipeline execution
- Automated retries and failure handling
- Polyglot persistence architecture
- Observability through logging and monitoring
- Modular and scalable workflow design

---

## Academic Context

This project was developed as part of a data engineering and orchestration coursework assignment. The focus was on designing a realistic ETL pipeline with automation, monitoring, and analytics capabilities.

---

## Future Improvements

Potential enhancements include:

- Cloud deployment (AWS, Azure, or GCP)
- Docker containerization
- CI/CD pipeline integration
- Streaming data ingestion
- Natural language processing for transcript insights




