# MeetingBank Data Engineering Project  
Group 2 – Data Orchestrators  

## Overview

This project presents a fully automated data engineering pipeline for processing municipal meeting transcript data. The system is designed to demonstrate end-to-end ETL workflow orchestration, polyglot data storage, and analytics visualization.

The pipeline extracts transcript data, performs transformation and feature engineering, stores structured and unstructured components in separate databases, and provides analytical insights through an interactive dashboard.

This project was developed as part of a Data Engineering and Orchestration coursework assignment.

---

## System Design

The architecture follows a layered design:

1. Orchestration Layer – Prefect manages workflow scheduling, retries, and monitoring.
2. Processing Layer – Python and Pandas perform data extraction, cleaning, and feature engineering.
3. Storage Layer (Polyglot Architecture)
   - MySQL stores structured metadata.  
   - MongoDB stores full meeting transcripts.
4. Analytics Layer– Streamlit provides interactive visualization of processed data.
5. Monitoring Layer – Prefect logs and deployment dashboards provide execution tracking.

This separation ensures scalability, maintainability, and optimized query performance.

---

## Requirements

Before running the project, ensure the following are installed:

- Python 3.9 or higher  
- MySQL Server (running locally)  
- MongoDB Community Edition (running locally)  

Install Python dependencies:
pip install prefect pandas sqlalchemy pymysql pymongo streamlit

Alternatively, install using:
pip install -r Requirements.txt

---

## Execution Instructions

### Run the ETL Pipeline
python meeting_pipeline_flow1.py

This will:
- Execute the Prefect workflow
- Load structured data into MySQL
- Load transcripts into MongoDB
- Register automated deployments

### Launch the Dashboard
streamlit run app.py


Access the dashboard at:
http://localhost:8501

---

## Project Structure
Data Engineering/
│
├── 1_Source_Code/
│ ├── app.py
│ ├── meeting_pipeline_flow1.py
│ ├── parse_meetingbank.py
│ └── insert_mongodb.py
│
├── 2_Architecture_and_Database/
│ ├── Architecture Diagram.png
│ └── Database verification screenshots
│
├── 3_Prefect_Automation_Proof/
│ ├── Deployment dashboards
│ ├── Event feed monitoring
│ ├── Execution logs
│ └── Workflow run status screenshots
│
├── 4_Streamlit_Dashboard_Results/
│ ├── Analytics dashboard outputs
│ ├── Meeting distribution analysis
│ ├── Content depth analytics
│ └── Data quality validation charts
│
├── 5_Report/
│ └── Final_Project_Report.pdf
│
├── Data/
│ └── meetings_metadata_FIXED.csv
│
├── Requirements.txt
├── Presentation.pptx
└── README.md

---

## Key Concepts Demonstrated

- ETL pipeline design  
- Workflow orchestration using Prefect  
- Polyglot persistence (MySQL + MongoDB)  
- Automated scheduling and monitoring  
- Feature engineering for analytics  
- Dashboard-based data visualization  
- Idempotent pipeline execution  

---

## Academic Context

This project was completed as part of a Data Engineering course to demonstrate practical implementation of automated workflows, database integration, and analytics reporting within a structured architectural framework.

---




