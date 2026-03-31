Excel to SQL ETL API (FastAPI + Python + SQL Server)
Overview

This project implements a data ingestion and ETL API service that allows users to upload Excel data, validate it, and load it into a SQL Server database.

The system exposes REST APIs to manage data import, validation, and reprocessing, making it suitable for automated reporting pipelines and enterprise data workflows.

 Features

🔹 REST API built with FastAPI for data ingestion

🔹 Upload and process Excel files dynamically

🔹 Data cleaning and transformation using pandas

🔹 Bulk data insertion using optimized SQL operations

🔹 Connection pooling for efficient database access

🔹 Data validation before insertion

🔹 Duplicate detection using submission period logic

🔹 Reprocessing capability (delete + reinsert data)

🔹 Progress tracking for long-running operations

🔹 Import history tracking

 Architecture
Client (API Request)
        ↓
FastAPI Backend
        ↓
Data Processing (pandas)
        ↓
SQL Server Database
 Technologies Used

Python

FastAPI

pandas

NumPy

pyodbc

SQL Server

OpenPyXL

 ETL Workflow
1. Extract

Reads Excel files from user-provided file paths

Supports multiple datasets and table mappings

2. Transform

Cleans data (handles null values, formatting)

Standardizes dataset structure

Validates submission periods

3. Load

Inserts data into SQL Server using batch operations

Performs duplicate checks before insertion

Supports data reprocessing (delete + reload)

 API Endpoints
 Connect to Database
POST /connect_database
 Import Excel Data
POST /import-excel
 Check Existing Data
POST /check-excel
 Check & Reinsert Data
POST /check-and-insert-excel
 Get Import History
GET /imported-files
Track Progress
GET /progress
