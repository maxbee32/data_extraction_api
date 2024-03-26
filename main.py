import datetime
import numpy as np
import pandas as pd
import pyodbc
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from operator import itemgetter

app = FastAPI()

# Define your database connection parameters
imported_files = []


# Define Pydantic model for database credentials
class DatabaseCredentials(BaseModel):
    server: str
    database: str
    username: str
    password: str


class ConnectionPool:
    def __init__(self, max_connections=5):
        self.max_connections = max_connections
        self.connections = []

    @staticmethod
    def create_connection(credentials):
        return pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                              f'SERVER={credentials.server};'
                              f'DATABASE={credentials.database};'
                              f'UID={credentials.username};'
                              f'PWD={credentials.password}')

    def initialize_pool(self, credentials):
        for _ in range(self.max_connections):
            connection = self.create_connection(credentials)
            self.connections.append(connection)

    def get_connection(self):
        if self.connections:
            return self.connections.pop(0)
        else:
            return None

    def release_connection(self, connection):
        self.connections.append(connection)


connection_pool = None


@app.on_event("startup")
async def startup_event():
    global connection_pool
    # Initialize the connection pool with your database credentials
    # Here, you can provide default values or initialize with empty credentials
    connection_pool = ConnectionPool()
    # Credentials will be provided dynamically when connecting


@app.post("/connect_database")
async def connect_database(credentials: DatabaseCredentials):
    global connection_pool, connection, cursor
    try:
        # Initialize the connection pool with the received credentials
        connection_pool.initialize_pool(credentials)

        # Get a connection from the pool
        connection = connection_pool.get_connection()
        if not connection:
            return Response(content="Connection pool exhausted", status_code=400)

        cursor = connection.cursor()

        # Return success message if connection is established
        return Response(content="Database connection established successfully", status_code=200)
    except Exception as e:
        # Return error message if connection fails
        return Response(content=f"Failed to establish database connection: {e}", status_code=400)
    finally:
        # Release the connection back to the pool
        if connection:
            connection_pool.release_connection(connection)


# # Store progress percentage globally
# progress_percentage = 0


# Define a loading screen class
class LoadingScreen:
    def __init__(self):
        self.progress_percentage = 0

    def update_percentage(self, percentage):
        self.progress_percentage = percentage


# Pydantic model for request body
class ExcelData(BaseModel):
    file_path: str
    table_name: str
    reinsert_data: str


# # Endpoint to provide information about all imported files
# @app.get("/imported-files", status_code=200)
# async def get_imported_files():
#     return {"imported_files": imported_files}


@app.get("/imported-files", status_code=200)
async def get_imported_files():
    sorted_files = sorted(imported_files, key=itemgetter("import_date"))
    return {"imported_files": sorted_files}
    # return JSONResponse(content={"imported_files": sorted_files}, status_code=200)


@app.post("/import-excel")
def insert_excel_data(data: ExcelData):
    global sql_command, progress_percentage
    try:
        if cursor is None:
            return Response(content="Database connection not established", status_code=500)

        # Read Excel data using pandas
        df = pd.read_excel(data.file_path, engine='openpyxl')
        total_rows = len(df)

        # Handle NULL values in the DataFrame
        df.replace({np.nan: None}, inplace=True)

        # Reset progress percentage
        progress_percentage = 0

        # Get current date and time
        current_date = datetime.datetime.now()
        formatted_datetime = current_date.strftime("%Y-%m-%d %H:%M:%S")

        # Insert data into SQL Server database
        for index, row in df.iterrows():
            # print(f"Inserting values: {tuple(row)}")
            # Construct your SQL insert command based on the provided table name
            if data.table_name == "tbl_C4C_Cube_Loans":
                sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

            elif data.table_name == "tbl_Landed_Property_Cube_Loans":
                sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

            elif data.table_name == "tbl_Cash_Denomination":
                sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

            elif data.table_name == "tbl_Payroll_Loan":
                sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

            elif data.table_name == "tbl_Savings":
                sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

            elif data.table_name == "tbl_Staff":
                sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

            elif data.table_name == "tbl_MNB800_Related_Party":
                sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

            # Execute the SQL command using executemany()
        cursor.fast_executemany = True
        cursor.executemany(sql_command, [tuple(row) for row in df.values])

        # Simulate processing time
        #     time.sleep(0.1)

        # Calculate progress percentage
        progress_percentage = int((index + 1) / total_rows * 100)

        # Commit the changes
        connection.commit()

        # Add information about the imported file to the list
        imported_files.append({"file_path": data.file_path, "table_name": data.table_name,
                               "import_date": formatted_datetime,
                               "status": "Imported"})

        return Response(content=f"Data imported to {data.table_name} successfully", status_code=200)
    except Exception as e:
        return Response(content=f"Error importing data to SQL Server: {e}", status_code=400)


# Endpoint to provide progress updates
@app.get("/progress")
async def get_progress():
    global progress_percentage
    return {"progress": progress_percentage}


@app.post("/check-excel")
def check_excel_data(data: ExcelData):
    try:
        # Check if database connection is established
        if cursor is None:
            raise HTTPException(status_code=500, detail="Database connection not established")

        # Read Excel data using pandas
        df = pd.read_excel(data.file_path, engine='openpyxl')

        # Handle NULL values in the DataFrame
        df.replace({np.nan: None}, inplace=True)

        # Check if data already exists in the table
        submission_periods = df['Submission_Period'].tolist()
        formatted_submission_periods = [f"'{period}'" for period in submission_periods]
        condition_values = ','.join(formatted_submission_periods)

        cursor.execute(f"SELECT COUNT(*) FROM {data.table_name} WHERE Submission_Period IN ({condition_values})")
        existing_rows = cursor.fetchone()[0]

        if existing_rows > 0:
            return Response(content="Data already exists in the table.Do you want to reinsert data? (yes / no)",
                            status_code=200)
    except Exception as e:
        return Response(content=f"Error checking data: {e}", status_code=200)


@app.post("/check-and-insert-excel")
def check_and_insert_excel_data(data: ExcelData):
    global sql_command, progress_percentage
    try:
        # Check if database connection is established
        if cursor is None:
            raise HTTPException(status_code=500, detail="Database connection not established")

        # Read Excel data using pandas
        df = pd.read_excel(data.file_path, engine='openpyxl')
        total_rows = len(df)

        # Handle NULL values in the DataFrame
        df.replace({np.nan: None}, inplace=True)

        # Reset progress percentage
        progress_percentage = 0

        # Check if data already exists in the table
        submission_periods = df['Submission_Period'].tolist()
        formatted_submission_periods = [f"'{period}'" for period in submission_periods]
        condition_values = ','.join(formatted_submission_periods)
        cursor.execute(f"SELECT COUNT(*) FROM {data.table_name} WHERE Submission_Period IN ({condition_values})")
        existing_rows = cursor.fetchone()[0]

        if existing_rows > 0 and data.reinsert_data != "yes":
            return Response(content="Sorry, Data not inserted.",status_code=200)

        else:
            # Delete existing data from the table
            cursor.execute(f"DELETE FROM {data.table_name} WHERE Submission_Period IN ({condition_values})")

            # Get current date and time
            current_date = datetime.datetime.now()
            formatted_datetime = current_date.strftime("%Y-%m-%d %H:%M:%S")

            # Insert data into SQL Server database
            for index, row in df.iterrows():
                # print(f"Inserting values: {tuple(row)}")
                # Construct your SQL insert command based on the provided table name
                if data.table_name == "tbl_C4C_Cube_Loans":
                    sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

                elif data.table_name == "tbl_Landed_Property_Cube_Loans":
                    sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

                elif data.table_name == "tbl_Cash_Denomination":
                    sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

                elif data.table_name == "tbl_Payroll_Loan":
                    sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

                elif data.table_name == "tbl_Savings":
                    sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

                elif data.table_name == "tbl_Staff":
                    sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

                elif data.table_name == "tbl_MNB800_Related_Party":
                    sql_command = f"INSERT INTO {data.table_name} VALUES ({','.join(['?'] * len(df.columns))})"

                # Execute the SQL command using executemany()
            cursor.fast_executemany = True
            cursor.executemany(sql_command, [tuple(row) for row in df.values])

            # Calculate progress percentage
            progress_percentage = int((index + 1) / total_rows * 100)

            # Commit the changes
        connection.commit()

        # Add information about the imported file to the list
        imported_files.append({"file_path": data.file_path, "table_name": data.table_name,
                               "import_date": formatted_datetime,
                               "status": "Re-imported"})

        return Response(content=f"Data re-imported to {data.table_name} successfully", status_code=200)
    except Exception as e:
        return Response(content=f"Error re-importing data to SQL Server: {e}", status_code=400)
