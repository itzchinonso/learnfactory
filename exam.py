# # # # 

# # # import psycopg2
# # # import csv
# # # from io import StringIO  # Import StringIO

# # # def copy_from_csv(conn, table_name, csv_file_path, delimiter=',', null_string=''):
# # #     """
# # #     Copies data from a CSV file into a PostgreSQL table using the COPY command.
# # #     This is the most efficient way to load large amounts of data.

# # #     Args:
# # #         conn: psycopg2 connection object.
# # #         table_name (str): The name of the table to copy data into.
# # #         csv_file_path (str): Path to the CSV file.
# # #         delimiter (str, optional): The delimiter used in the CSV file. Defaults to ','.
# # #         null_string (str, optional): The string representing NULL values in the CSV. Defaults to ''.
# # #     """
# # #     cursor = conn.cursor()
# # #     try:
# # #         with open(csv_file_path, 'r') as f:
# # #             # 1.  Use StringIO to wrap the file object.  This is crucial for copy_from.
# # #             csv_file = StringIO(f.read())

# # #             # 2.  Important:  Set the file's read position to the beginning.
# # #             csv_file.seek(0)

# # #             # 3.  Use copy_from.  Note the explicit 'file=' argument.
# # #             cursor.copy_from(csv_file, table_name, sep=delimiter, null=null_string)
# # #             conn.commit()
# # #             print(f"Data from '{csv_file_path}' successfully copied to table '{table_name}'.")

# # #     except psycopg2.Error as e:
# # #         print(f"Error copying data from CSV to table: {e}")
# # #         conn.rollback()  # Rollback the transaction on error
# # #         raise  # Re-raise the exception to be handled by the caller, if needed.
# # #     finally:
# # #         cursor.close()  # Ensure the cursor is closed.

# # # def create_table_from_csv(conn, table_name, csv_file_path, delimiter=',', null_string=''):
# # #     """
# # #     Creates a PostgreSQL table from a CSV file, inferring column names and data types
# # #     from the CSV file's header row.  Handles potential issues with empty fields.

# # #     Args:
# # #         conn: psycopg2 connection object.
# # #         table_name (str): The name of the table to create.
# # #         csv_file_path (str): Path to the CSV file.
# # #         delimiter (str, optional): The delimiter used in the CSV file. Defaults to ','.
# # #         null_string (str, optional):  The string representing NULL values in the CSV.
# # #     """
# # #     cursor = conn.cursor()
# # #     try:
# # #         with open(csv_file_path, 'r') as f:
# # #             reader = csv.reader(f, delimiter=delimiter)
# # #             header = next(reader)  # Get the header row
# # #             # Determine data types from the first row of data (after the header)
# # #             first_data_row = next(reader, None)  # Get the first data row or None if empty

# # #             if not first_data_row:
# # #                 raise ValueError("CSV file is empty or contains only a header.")

# # #             # Infer data types.  Handles empty strings correctly now.
# # #             column_types = []
# # #             for value in first_data_row:
# # #                 if value == null_string:  # Check for your NULL string
# # #                     column_types.append('TEXT')  # Default to TEXT for NULLs
# # #                 else:
# # #                     try:
# # #                         int(value)
# # #                         column_types.append('INTEGER')
# # #                     except ValueError:
# # #                         try:
# # #                             float(value)
# # #                             column_types.append('REAL')
# # #                         except ValueError:
# # #                             column_types.append('TEXT')  # Fallback to TEXT

# # #         # Construct the CREATE TABLE statement.
# # #         columns = [f"{header[i]} {column_types[i]}" for i in range(len(header))]
# # #         create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
# # #         print(f"Creating table: {create_table_query}")  # Print the query
# # #         cursor.execute(create_table_query)
# # #         conn.commit()
# # #         print(f"Table '{table_name}' successfully created.")

# # #     except psycopg2.Error as e:
# # #         print(f"Error creating table: {e}")
# # #         conn.rollback()
# # #         raise
# # #     except ValueError as e:
# # #         print(f"Error determining column types: {e}")
# # #         conn.rollback()
# # #         raise
# # #     finally:
# # #         cursor.close()


# # # def main():
# # #     """
# # #     Main function to connect to the database, create a table, and copy data from a CSV file.
# # #     """
# # #     # 1.  Database connection details (replace with your actual details)
# # #     dbname = "rewards_data"  # Replace with your database name
# # #     user = "postgres"  # Default PostgreSQL user
# # #     password = "uche123"
# # #     host = "localhost"  # e.g., 'localhost' or an IP address
# # #     port = "5432"  # Default PostgreSQL port

# # #     # 2. CSV file path and table name
# # #     csv_file_path = "RewardsData.csv"  # Replace with your CSV file path
# # #     table_name = "reward_data"  # Replace with your desired table name

# # #     try:
# # #         # 3. Establish database connection
# # #         conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
# # #         conn.autocommit = False # Start a transaction

# # #         # 4. Create the table (if it doesn't exist)
# # #         create_table_from_csv(conn, table_name, csv_file_path)

# # #         # 5. Copy data from the CSV file to the table
# # #         copy_from_csv(conn, table_name, csv_file_path)

# # #         conn.commit() # Explicitly commit the transaction
# # #         print("Transaction completed successfully.")

# # #     except psycopg2.Error as e:
# # #         print(f"Database error: {e}")
# # #         # IMPORTANT:  No conn.rollback() here.  It's handled in the functions.
# # #     except Exception as e:
# # #         print(f"An unexpected error occurred: {e}")
# # #         # No conn.rollback() here either.
# # #     finally:
# # #         if conn:
# # #             conn.close()
# # #             print("Connection closed.")

# # # if _name_ == "_main_":
# # #     main()



# # import psycopg2
# # import csv
# # from io import StringIO  # Import StringIO

# # def copy_from_csv(conn, table_name, csv_file_path, delimiter=',', null_string=''):
# #     """
# #     Copies data from a CSV file into a PostgreSQL table using the COPY command.
# #     This is the most efficient way to load large amounts of data.

# #     Args:
# #         conn: psycopg2 connection object.
# #         table_name (str): The name of the table to copy data into.
# #         csv_file_path (str): Path to the CSV file.
# #         delimiter (str, optional): The delimiter used in the CSV file. Defaults to ','.
# #         null_string (str, optional): The string representing NULL values in the CSV. Defaults to ''.
# #     """
# #     cursor = conn.cursor()
# #     try:
# #         with open(csv_file_path, 'r') as f:
# #             reader = csv.reader(f, delimiter=delimiter)
# #             header = next(reader)  # Read and discard the header row
# #             csv_file = StringIO()
# #             writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)
# #             for row in reader:
# #                 # Check if the row has more columns than the header
# #                 if len(row) > len(header):
# #                     row = row[:len(header)]  # Truncate the row to match the header
# #                 elif len(row) < len(header):
# #                     # Pad the row with empty strings if it has fewer columns
# #                     row += [''] * (len(header) - len(row))
# #                 writer.writerow(row)
# #             csv_file.seek(0)

# #             cursor.copy_from(csv_file, table_name, sep=delimiter, null=null_string)
# #             conn.commit()
# #             print(f"Data from '{csv_file_path}' successfully copied to table '{table_name}'.")

# #     except psycopg2.Error as e:
# #         print(f"Error copying data from CSV to table: {e}")
# #         conn.rollback()  # Rollback the transaction on error
# #         raise  # Re-raise the exception to be handled by the caller, if needed.
# #     finally:
# #         cursor.close()  # Ensure the cursor is closed.
                
# # def create_table_from_csv(conn, table_name, csv_file_path, delimiter=',', null_string=''):
# #     """
# #     Creates a PostgreSQL table from a CSV file, inferring column names and data types
# #     from the CSV file's header row.  Handles potential issues with empty fields.

# #     Args:
# #         conn: psycopg2 connection object.
# #         table_name (str): The name of the table to create.
# #         csv_file_path (str): Path to the CSV file.
# #         delimiter (str, optional): The delimiter used in the CSV file. Defaults to ','.
# #         null_string (str, optional):  The string representing NULL values in the CSV.
# #     """
# #     cursor = conn.cursor()
# #     try:
# #         with open(csv_file_path, 'r') as f:
# #             reader = csv.reader(f, delimiter=delimiter)
# #             header = next(reader)  # Get the header row
# #             # Determine data types from the first row of data (after the header)
# #             first_data_row = next(reader, None)  # Get the first data row or None if empty

# #             if not first_data_row:
# #                 raise ValueError("CSV file is empty or contains only a header.")

# #             # Infer data types.  Handles empty strings correctly now.
# #             column_types = []
# #             for value in first_data_row:
# #                 if value == null_string:  # Check for your NULL string
# #                     column_types.append('TEXT')  # Default to TEXT for NULLs
# #                 else:
# #                     try:
# #                         int(value)
# #                         column_types.append('INTEGER')
# #                     except ValueError:
# #                         try:
# #                             float(value)
# #                             column_types.append('REAL')
# #                         except ValueError:
# #                             column_types.append('TEXT')  # Fallback to TEXT

# #         # Construct the CREATE TABLE statement.
# #         columns = [f"{header[i]} {column_types[i]}" for i in range(len(header))]
# #         create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
# #         print(f"Creating table: {create_table_query}")  # Print the query
# #         cursor.execute(create_table_query)
# #         conn.commit()
# #         print(f"Table '{table_name}' successfully created.")

# #     except psycopg2.Error as e:
# #         print(f"Error creating table: {e}")
# #         conn.rollback()
# #         raise
# #     except ValueError as e:
# #         print(f"Error determining column types: {e}")
# #         conn.rollback()
# #         raise
# #     finally:
# #         cursor.close()


# # def main():
# #     """
# #     Main function to connect to the database, create a table, and copy data from a CSV file.
# #     """
# #     # 1.  Database connection details (replace with your actual details)
# #     dbname = "rewards_data"  # Replace with your database name
# #     user = "postgres"  # Default PostgreSQL user
# #     password = "uche123"
# #     host = "localhost"  # e.g., 'localhost' or an IP address
# #     port = "5432"  # Default PostgreSQL port

# #     # 2. CSV file path and table name
# #     csv_file_path = "RewardsData.csv"  # Replace with your CSV file path
# #     table_name = "reward_data"  # Replace with your desired table name

# #     try:
# #         # 3. Establish database connection
# #         conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
# #         conn.autocommit = False # Start a transaction

# #         # 4. Create the table (if it doesn't exist)
# #         create_table_from_csv(conn, table_name, csv_file_path)

# #         # 5. Copy data from the CSV file to the table
# #         copy_from_csv(conn, table_name, csv_file_path)

# #         conn.commit() # Explicitly commit the transaction
# #         print("Transaction completed successfully.")

# #     except psycopg2.Error as e:
# #         print(f"Database error: {e}")
# #         # IMPORTANT:  No conn.rollback() here.  It's handled in the functions.
# #     except Exception as e:
# #         print(f"An unexpected error occurred: {e}")
# #         # No conn.rollback() here either.
# #     finally:
# #         if conn:
# #             conn.close()
# #             print("Connection closed.")

# # if _name_ == "_main_":
# #     main()



# import psycopg2
# import csv
# from io import StringIO  # Import StringIO

# def copy_from_csv(conn, table_name, csv_file_path, delimiter=',', null_string=''):
#     """
#     Copies data from a CSV file into a PostgreSQL table using the COPY command.
#     This is the most efficient way to load large amounts of data.

#     Args:
#         conn: psycopg2 connection object.
#         table_name (str): The name of the table to copy data into.
#         csv_file_path (str): Path to the CSV file.
#         delimiter (str, optional): The delimiter used in the CSV file. Defaults to ','.
#         null_string (str, optional): The string representing NULL values in the CSV. Defaults to ''.
#     """
#     cursor = conn.cursor()
#     try:
#         with open(csv_file_path, 'r') as f:
#             reader = csv.reader(f, delimiter=delimiter)
#             header = next(reader)  # Read and discard the header row
#             csv_file = StringIO()
#             writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)
#             for row in reader:
#                 # Check if the row has more columns than the header
#                 if len(row) > len(header):
#                     row = row[:len(header)]  # Truncate the row to match the header
#                 elif len(row) < len(header):
#                     # Pad the row with empty strings if it has fewer columns
#                     row += [''] * (len(header) - len(row))
#                 writer.writerow(row)
#             csv_file.seek(0)

#             cursor.copy_from(csv_file, table_name, sep=delimiter, null=null_string)
#             conn.commit()
#             print(f"Data from '{csv_file_path}' successfully copied to table '{table_name}'.")

#     except psycopg2.Error as e:
#         print(f"Error copying data from CSV to table: {e}")
#         conn.rollback()  # Rollback the transaction on error
#         raise  # Re-raise the exception to be handled by the caller, if needed.
#     finally:
#         cursor.close()  # Ensure the cursor is closed.

# def create_table_from_csv(conn, table_name, csv_file_path,  null_string=''):
#     """
#     Creates a PostgreSQL table from a CSV file, inferring column names and data types
#     from the CSV file's header row.  Handles potential issues with empty fields and
#     attempts to handle different CSV dialects.

#     Args:
#         conn: psycopg2 connection object.
#         table_name (str): The name of the table to create.
#         csv_file_path (str): Path to the CSV file.
#         null_string (str, optional): The string representing NULL values in the CSV.
#     """
#     cursor = conn.cursor()
#     try:
#         with open(csv_file_path, 'r') as f:
#             # Use csv.Sniffer to detect the delimiter and quotechar
#             dialect = csv.Sniffer().sniff(f.read(1024))  # Read a chunk to sniff
#             f.seek(0)  # Reset file position after sniffing
#             reader = csv.reader(f, dialect)
#             header = next(reader)  # Get the header row

#             # Determine data types from the first row of data (after the header)
#             first_data_row = next(reader, None)  # Get the first data row or None if empty
#             if not first_data_row:
#                 raise ValueError("CSV file is empty or contains only a header.")
#              # Infer data types.  Handles empty strings correctly now.
#             column_types = []
#             for value in first_data_row:
#                 if value == null_string:  # Check for your NULL string
#                     column_types.append('TEXT')  # Default to TEXT for NULLs
#                 else:
#                     try:
#                         int(value)
#                         column_types.append('INTEGER')
#                     except ValueError:
#                         try:
#                             float(value)
#                             column_types.append('REAL')
#                         except ValueError:
#                             column_types.append('TEXT')  # Fallback to TEXT

#         # Construct the CREATE TABLE statement.
#         columns = [f"{header[i]} {column_types[i]}" for i in range(len(header))]
#         create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
#         print(f"Creating table: {create_table_query}")  # Print the query
#         cursor.execute(create_table_query)
#         conn.commit()
#         print(f"Table '{table_name}' successfully created.")

#     except psycopg2.Error as e:
#         print(f"Error creating table: {e}")
#         conn.rollback()
#         raise
#     except ValueError as e:
#         print(f"Error determining column types or CSV format: {e}")
#         conn.rollback()
#         raise
#     finally:
#         cursor.close()


# def main():
#     """
#     Main function to connect to the database, create a table, and copy data from a CSV file.
#     """
#     # 1.  Database connection details (replace with your actual details)
#     dbname = "rewards_data"  # Replace with your database name
#     user = "postgres"  # Default PostgreSQL user
#     password = "uche123"
#     host = "localhost"  # e.g., 'localhost' or an IP address
#     port = "5432"  # Default PostgreSQL port

#     # 2. CSV file path and table name
#     csv_file_path = "RewardsData.csv"  # Replace with your CSV file path
#     table_name = "reward_data"  # Replace with your desired table name

#     try:
#         # 3. Establish database connection
#         conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
#         conn.autocommit = False # Start a transaction

#         # 4. Create the table (if it doesn't exist)
#         create_table_from_csv(conn, table_name, csv_file_path)

#         # 5. Copy data from the CSV file to the table
#         copy_from_csv(conn, table_name, csv_file_path)

#         conn.commit() # Explicitly commit the transaction
#         print("Transaction completed successfully.")

#     except psycopg2.Error as e:
#         print(f"Database error: {e}")
#         # IMPORTANT:  No conn.rollback() here.  It's handled in the functions.
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         # No conn.rollback() here either.
#     finally:
#         if conn:
#             conn.close()
#             print("Connection closed.")

# if _name_ == "_main_":
#     main()



# import psycopg2
# from psycopg2 import sql
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # Recommended for CREATE DATABASE/SCHEMA
# from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
# import sys

# import psycopg2.extras # Import sys for exiting if database creation fails

# # --- Database Connection Parameters ---
# # Replace with your actual database credentials for BOTH databases

# # Connection details for the SOURCE database (dvdrental)
# SOURCE_DB_HOST = "localhost" # e.g., "localhost" or an IP address
# SOURCE_DB_NAME = "dvd_rentals" # The database containing the film table
# SOURCE_DB_USER = "postgres" # Your PostgreSQL username for dvdrental
# SOURCE_DB_PASSWORD = "uche123" # Your PostgreSQL password for dvdrental
# SOURCE_DB_PORT = "5432" # Your PostgreSQL port

# # Connection details for the TARGET database (my_database)
# TARGET_DB_HOST = "localhost" # Usually the same host as dvdrental, but can be different
# TARGET_DB_NAME = "my_database" # The database you want to create/connect to
# TARGET_DB_USER = "postgres" # Your PostgreSQL username for my_database (can be the same as dvdrental user)
# TARGET_DB_PASSWORD = "uche123" # Your PostgreSQL password for my_database
# TARGET_DB_PORT = "5432" # Your PostgreSQL port

# # Define the source schema/table
# SOURCE_SCHEMA = "public" # The schema where the 'film' table is located in dvdrental
# SOURCE_FILM_TABLE = "film"

# # Define the new schema and tables within my_database
# NEW_SCHEMA = "new_schema"
# TARGET_FILM_TABLE = "film1"
# TARGET_DATA_TABLE = "my_data"

# # Define the structure of the film table from dvdrental for recreation in my_database
# # This is a common structure for the dvdrental film table.
# # We define it manually here to create the table in the target database.
# FILM_TABLE_SCHEMA_SQL = """
#     CREATE TABLE {schema}.{table} (
#         film_id INTEGER PRIMARY KEY,
#         title VARCHAR(255) NOT NULL,
#         description TEXT,
#         release_year INTEGER,
#         language_id SMALLINT NOT NULL,
#         rental_duration SMALLINT NOT NULL,
#         rental_rate NUMERIC(4,2) NOT NULL,
#         length SMALLINT,
#         replacement_cost NUMERIC(5,2) NOT NULL,
#         rating VARCHAR(10), -- Can be enum or text depending on dvdrental setup
#         last_update TIMESTAMP WITHOUT TIME ZONE NOT NULL,
#         special_features TEXT[], -- This is an array type
#         fulltext TSVECTOR NOT NULL
#     );
# """

# # --- Database Creation Function (Connects to 'postgres' db) ---
# def create_database_if_not_exists(db_host, db_user, db_password, db_port, new_db_name):
#     """Connects to the default maintenance database (postgres) to create a new database."""
#     conn = None
#     try:
#         # Connect to the default 'postgres' database to be able to create a new database
#         print(f"Attempting to connect to 'postgres' database at {db_host}:{db_port} as user {db_user}...")
#         conn = psycopg2.connect(host=db_host, database="postgres", user=db_user, password=db_password, port=db_port)
#         conn.autocommit = True
#         cursor = conn.cursor()

#         # Check if the database already exists
#         cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), (new_db_name,))
#         exists = cursor.fetchone()

#         if not exists:
#             print(f"Database '{new_db_name}' does not exist. Creating...")
#             # Create the new database
#             cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(new_db_name)))
#             print(f"Database '{new_db_name}' created successfully.")
#         else:
#             print(f"Database '{new_db_name}' already exists.")

#         cursor.close()
#         return True # Indicate success

#     except psycopg2.OperationalError as e:
#         print(f"Operational error while connecting to 'postgres' or creating '{new_db_name}': {e}")
#         print("Please ensure PostgreSQL is running, your connection parameters for the default database are correct, and the user has privileges to create databases.")
#         return False # Indicate failure
#     except Exception as e:
#         print(f"An unexpected error occurred during database creation check: {e}")
#         return False # Indicate failure
#     finally:
#         if conn:
#             conn.close()

# # --- Function to fetch data and aggregates from the source database ---
# def fetch_data_from_source(db_host, db_name, db_user, db_password, db_port, source_schema, source_table):
#     """Connects to the source database and fetches film data and aggregate statistics."""
#     conn = None
#     cursor = None
#     film_data = []
#     aggregate_stats = None

#     try:
#         print(f"\nConnecting to source database '{db_name}'...")
#         conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password, port=db_port)
#         print("Source database connection successful.")

#         cursor = conn.cursor()

#         # Fetch all data from the film table
#         print(f"Fetching data from {source_schema}.{source_table}...")
#         fetch_film_sql = sql.SQL("SELECT * FROM {schema}.{table};").format(
#             schema=sql.Identifier(source_schema),
#             table=sql.Identifier(source_table)
#         )
#         cursor.execute(fetch_film_sql)
#         film_data = cursor.fetchall()
#         print(f"Fetched {len(film_data)} rows from {source_schema}.{source_table}.")

#         # Calculate aggregate statistics
#         print(f"Calculating aggregate statistics from {source_schema}.{source_table}...")
#         calculate_stats_sql = sql.SQL("""
#             SELECT
#                 COUNT(*) AS tot_row,
#                 COUNT(DISTINCT rating) AS dist_rating,
#                 SUM(length) AS tot_length,
#                 AVG(length),
#                 ROUND(AVG(length)::NUMERIC, 2) AS round_avg
#             FROM {schema}.{table};
#         """).format(
#              schema=sql.Identifier(source_schema),
#              table=sql.Identifier(source_table)
#         )
#         cursor.execute(calculate_stats_sql)
#         aggregate_stats = cursor.fetchone()
#         print("Aggregate statistics calculated.")

#         return film_data, aggregate_stats # Return fetched data and stats

#     except psycopg2.OperationalError as e:
#         print(f"Operational error while connecting to source database '{db_name}': {e}")
#         print("Please ensure the source database is accessible and connection parameters are correct.")
#         return None, None # Indicate failure
#     except psycopg2.ProgrammingError as e:
#         print(f"Database programming error while fetching from source: {e}")
#         return None, None # Indicate failure
#     except Exception as e:
#         print(f"An unexpected error occurred while fetching from source: {e}")
#         return None, None # Indicate failure
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#             print("Source database connection closed.")

# # --- Function to setup target database and insert data ---
# def setup_target_database_and_insert(db_host, db_name, db_user, db_password, db_port, new_schema, target_film_table, target_data_table, film_data, aggregate_stats):
#     """Connects to the target database and inserts the fetched data and statistics."""
#     conn = None
#     cursor = None
#     try:
#         print(f"\nConnecting to target database '{db_name}'...")
#         conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password, port=db_port)
#         conn.set_isolation_level(1) # Set isolation level
#         print("Target database connection successful.")

#         cursor = conn.cursor()

#         # Create new schema if it doesn't exist
#         print(f"Creating schema '{new_schema}' if it doesn't exist...")
#         cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(new_schema)))
#         print(f"Schema '{new_schema}' created or already exists.")

#         # Drop tables if they exist to allow rerunning the script easily
#         print(f"Dropping existing tables '{new_schema}.{target_film_table}' and '{new_schema}.{target_data_table}' if they exist...")
#         cursor.execute(sql.SQL("DROP TABLE IF EXISTS {schema}.{film_table};").format(
#             schema=sql.Identifier(new_schema), film_table=sql.Identifier(target_film_table)))
#         cursor.execute(sql.SQL("DROP TABLE IF EXISTS {schema}.{data_table};").format(
#             schema=sql.Identifier(new_schema), data_table=sql.Identifier(target_data_table)))
#         print("Existing tables dropped.")


#         # Create target film table
#         print(f"Creating table '{new_schema}.{target_film_table}'...")
#         create_film_table_sql = sql.SQL(FILM_TABLE_SCHEMA_SQL).format(
#              schema=sql.Identifier(new_schema),
#              table=sql.Identifier(target_film_table)
#         )
#         cursor.execute(create_film_table_sql)
#         print(f"Table '{new_schema}.{target_film_table}' created.")

#         # Insert film data into the target table
#         if film_data:
#             print(f"Inserting {len(film_data)} rows into '{new_schema}.{target_film_table}'...")
#             # Construct the INSERT statement with placeholders for all columns
#             # Assumes the order of columns in film_data matches the CREATE TABLE statement
#             insert_film_sql = sql.SQL("INSERT INTO {schema}.{table} VALUES ({values});").format(
#                 schema=sql.Identifier(new_schema),
#                 table=sql.Identifier(target_film_table),
#                 values=sql.SQL(', ').join(sql.Placeholder() * len(film_data[0])) # Create placeholders for each column
#             )
#             # Use executemany for efficient bulk insertion
#             cursor.executemany(insert_film_sql, film_data)
#             print(f"Successfully inserted {len(film_data)} rows.")
#         else:
#             print("No film data to insert.")


#         # Create 'my_data' table
#         print(f"Creating table '{new_schema}.{target_data_table}'...")
#         create_my_data_table_sql = sql.SQL("""
#             CREATE TABLE {schema}.{table} (
#                 tot_row INTEGER,
#                 dist_rating INTEGER,
#                 tot_length INTEGER,
#                 avg_length NUMERIC,
#                 round_avg NUMERIC(10, 2)
#             );
#         """).format(
#             schema=sql.Identifier(new_schema),
#             table=sql.Identifier(target_data_table)
#         )
#         cursor.execute(create_my_data_table_sql)
#         print(f"Table '{new_schema}.{target_data_table}' created.")

#         # Insert statistics into my_data table
#         if aggregate_stats:
#              print(f"Inserting statistics into '{new_schema}.{target_data_table}'...")
#              insert_stats_sql = sql.SQL("""
#                  INSERT INTO {schema}.{table} (tot_row, dist_rating, tot_length, avg_length, round_avg)
#                  VALUES (%s, %s, %s, %s, %s);
#              """).format(
#                  schema=sql.Identifier(new_schema),
#                  table=sql.Identifier(target_data_table)
#              )
#              cursor.execute(insert_stats_sql, aggregate_stats) # aggregate_stats is already a tuple
#              print("Statistics inserted successfully.")
#         else:
#              print("No aggregate statistics to insert.")


#         # Commit the transaction
#         conn.commit()
#         print("\nAll target database operations completed successfully and committed.")

#     except psycopg2.OperationalError as e:
#         print(f"Operational error while connecting to target database '{db_name}': {e}")
#         print("Please ensure the target database is accessible and connection parameters are correct.")
#         if conn:
#              conn.rollback() # Roll back the transaction on error
#              print("Transaction rolled back.")
#     except psycopg2.ProgrammingError as e:
#         print(f"Database programming error while setting up target database or inserting: {e}")
#         if conn:
#              conn.rollback() # Roll back the transaction on error
#              print("Transaction rolled back.")
#     except Exception as e:
#         print(f"An unexpected error occurred during target database operations: {e}")
#         if conn:
#              conn.rollback() # Roll back the transaction on error
#              print("Transaction rolled back.")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#             print("Target database connection closed.")

# # --- Main execution ---
# if _name_ == "_main_":
#     # 1. Ensure the target database exists
#     print("--- Starting Database Setup ---")
#     db_created_or_exists = create_database_if_not_exists(TARGET_DB_HOST, TARGET_DB_USER, TARGET_DB_PASSWORD, TARGET_DB_PORT, TARGET_DB_NAME)

#     if not db_created_or_exists:
#         print("\nExiting: Could not access or create the target database.")
#         sys.exit(1) # Exit if the target database is not ready

#     # 2. Fetch data and aggregates from the source database (dvdrental)
#     print("\n--- Fetching Data from Source Database ---")
#     film_data, aggregate_stats = fetch_data_from_source(SOURCE_DB_HOST, SOURCE_DB_NAME, SOURCE_DB_USER, SOURCE_DB_PASSWORD, SOURCE_DB_PORT, SOURCE_SCHEMA, SOURCE_FILM_TABLE)

#     if film_data is None or aggregate_stats is None:
#          print("\nExiting: Could not fetch data or statistics from the source database.")
#          sys.exit(1) # Exit if data fetching failed

#     # 3. Setup the target database and insert the fetched data
#     print("\n--- Setting up Target Database and Inserting Data ---")
#     setup_target_database_and_insert(TARGET_DB_HOST, TARGET_DB_NAME, TARGET_DB_USER, TARGET_DB_PASSWORD, TARGET_DB_PORT, NEW_SCHEMA, TARGET_FILM_TABLE, TARGET_DATA_TABLE, film_data, aggregate_stats)

#     print("\n--- Script Finished ---")


# 3

import tkinter as tk
from tkinter import scrolledtext, messagebox
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # For CREATE DATABASE
import os
import logging
import sys

# --- Configuration ---
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
LOG_FILE = "bank_processor.log"
TABLE_NAME = "largest_banks"
SCHEMA_NAME = "public" # Or another schema if you prefer

# PostgreSQL Database Connection Parameters
# You MUST replace these with your actual PostgreSQL credentials
DB_HOST = "localhost"
DB_NAME = "bank_data_db" # A new database name for this project
DB_USER = "postgres"
DB_PASSWORD = "chiemela"
DB_PORT = "5432"

# Exchange rates provided (USD to other currencies)
EXCHANGE_RATES = {
    "EUR": 0.93,  # $1 USD = 0.93 Euro
    "GBP": 0.80,  # $1 USD = 0.80 Pound
    "INR": 82.95  # $1 USD = 82.95 INR
}

# --- Logging Setup ---
# Create a custom handler to write logs to the GUI text widget
class TextWidgetHandler(logging.Handler):
    def _init_(self, text_widget):
        super()._init_()
        self.text_widget = text_widget
        self.text_widget.config(state='disabled') # Disable editing by user

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.config(state='normal') # Enable editing temporarily
        self.text_widget.insert(tk.END, msg + '\n')
        self.text_widget.see(tk.END) # Auto-scroll to the bottom
        self.text_widget.config(state='disabled') # Disable editing again

# Configure the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Set minimum logging level

# Create file handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO) # Log INFO and above to file

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add file handler to logger
logger.addHandler(file_handler)

# --- Database Utility Function (for checking/creating database) ---
def create_database_if_not_exists(db_host, db_user, db_password, db_port, new_db_name):
    """Connects to the default maintenance database (postgres) to create a new database."""
    conn = None
    try:
        # Connect to the default 'postgres' database to be able to create a new database
        logger.info(f"Attempting to connect to 'postgres' database at {db_host}:{db_port} as user {db_user} to check/create '{new_db_name}'...")
        conn = psycopg2.connect(host=db_host, database="postgres", user=db_user, password=db_password, port=db_port)

        # Explicitly set autocommit mode for this connection BEFORE executing CREATE DATABASE
        conn.autocommit = True

        cursor = conn.cursor()

        # Check if the database already exists
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), (new_db_name,))
        exists = cursor.fetchone()

        if not exists:
            logger.info(f"Database '{new_db_name}' does not exist. Creating...")
            # Create the new database - this command requires autocommit mode
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(new_db_name)))
            logger.info(f"Database '{new_db_name}' created successfully.")
        else:
            logger.info(f"Database '{new_db_name}' already exists.")

        cursor.close()
        return True # Indicate success

    except psycopg2.OperationalError as e:
        logger.error(f"Operational error while connecting to 'postgres' or creating '{new_db_name}': {e}")
        logger.error("Please ensure PostgreSQL is running, your connection parameters for the default database are correct, and the user has privileges to create databases.")
        return False # Indicate failure
    except Exception as e:
        logger.error(f"An unexpected error occurred during database creation check: {e}")
        return False # Indicate failure
    finally:
        if conn:
            conn.close()
            logger.info("Database creation check connection closed.")


# --- Core Logic Functions ---

def scrape_bank_data(url):
    """Scrapes bank data from the given URL."""
    logger.info(f"Attempting to scrape data from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        logger.info("Successfully fetched the page content.")

        # Use pandas to read HTML tables
        tables = pd.read_html(response.text)

        df = None
        for i, table in enumerate(tables):
            table_columns_lower = [col.lower() for col in table.columns]
            if any('rank' in col for col in table_columns_lower) and \
               any('bank name' in col for col in table_columns_lower) and \
               any('market cap' in col and ('us$' in col or 'usd' in col) for col in table_columns_lower):
                df = table
                logger.info(f"Identified the correct table (Table index {i}).")
                break

        if df is None:
            logger.error("Could not find the table with expected columns on the page.")
            return None

        # Clean up column names to make them easier to work with
        # Based on the URL, the columns are typically: Rank, Bank name, Market cap (US$ billion), Footnote
        # We need to be careful if the structure changes slightly.
        # Let's try to dynamically find the market cap column based on keywords
        market_cap_col = None
        for col in df.columns:
            if 'market cap' in col.lower() and ('us$' in col.lower() or 'usd' in col.lower()):
                market_cap_col = col
                break

        if market_cap_col is None:
             logger.error("Could not find the 'Market cap (US$ billion)' column.")
             return None

        # Rename columns for consistency
        df.rename(columns={
            df.columns[0]: 'Rank', # Assuming first column is Rank
            df.columns[1]: 'Bank name', # Assuming second column is Bank name
            market_cap_col: 'Market cap (US$ billion)'
        }, inplace=True)

        # Select the core columns we need
        df = df[['Rank', 'Bank name', 'Market cap (US$ billion)']]

        # Clean the 'Market cap (US$ billion)' column: remove commas and convert to numeric
        df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].astype(str).str.replace(',', '', regex=False)
        df['Market cap (US$ billion)'] = pd.to_numeric(df['Market cap (US$ billion)'], errors='coerce') # Coerce errors to NaN

        # Drop rows where Market Cap could not be converted (e.g., header rows repeated in the table)
        df.dropna(subset=['Market cap (US$ billion)'], inplace=True)

        # Convert Rank to integer, coercing errors
        df['Rank'] = pd.to_numeric(df['Rank'], errors='coerce').astype('Int64') # Use Int64 to allow NaN

        logger.info("Successfully scraped and cleaned initial data.")
        logger.info(f"Scraped data preview:\n{df.head()}")

        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching the URL: {e}")
        return None
    except Exception as e:
        logger.error(f"An error occurred during scraping: {e}")
        return None

def transform_data(df, rates):
    """Transforms the DataFrame by adding market cap in other currencies."""
    if df is None or df.empty:
        logger.warning("No data to transform.")
        return None

    logger.info("Starting data transformation...")

    transformed_df = df.copy() # Work on a copy to not modify the original DataFrame

    # Calculate market cap in other currencies
    try:
        # Ensure the USD column exists before calculating
        if 'Market cap (US$ billion)' not in transformed_df.columns:
            logger.error("USD market cap column not found for transformation.")
            return None

        transformed_df['Market cap (EUR billion)'] = (transformed_df['Market cap (US$ billion)'] * rates['EUR']).round(2)
        transformed_df['Market cap (GBP billion)'] = (transformed_df['Market cap (US$ billion)'] * rates['GBP']).round(2)
        transformed_df['Market cap (INR billion)'] = (transformed_df['Market cap (US$ billion)'] * rates['INR']).round(2)

        logger.info("Successfully transformed data.")
        logger.info(f"Transformed data preview:\n{transformed_df.head()}")

        return transformed_df

    except KeyError as e:
        logger.error(f"Missing exchange rate for currency: {e}")
        return None
    except Exception as e:
        logger.error(f"An error occurred during data transformation: {e}")
        return None

def load_to_database_psycopg2(df, db_host, db_name, db_user, db_password, db_port, schema_name, table_name):
    """Loads the DataFrame into the PostgreSQL database using psycopg2."""
    if df is None or df.empty:
        logger.warning("No data to load to database.")
        return

    # Ensure the target database exists first
    db_ready = create_database_if_not_exists(db_host, db_user, db_password, db_port, db_name)
    if not db_ready:
        logger.error(f"Target database '{db_name}' is not ready. Cannot proceed with loading.")
        return

    conn = None
    cursor = None
    try:
        logger.info(f"Attempting to connect to target database '{db_name}' at {db_host}:{db_port} as user {db_user}...")
        # Connect to the target database (which is now confirmed to exist)
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password, port=db_port)
        # Set isolation level for the data loading transaction
        # conn.set_isolation_level(psycopg2.extensions.READ_COMMITTED) # Using the constant name is preferred if available
        conn.autocommit = False # Start a transaction
        
        cursor = conn.cursor()
        logger.info("Database connection successful.")

        # Create schema if it doesn't exist (optional, but good practice)
        logger.info(f"Creating schema '{schema_name}' if it doesn't exist...")
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))
        logger.info(f"Schema '{schema_name}' created or already exists.")

        # Define the CREATE TABLE SQL statement
        # Ensure column names and types match the DataFrame columns
        create_table_sql = sql.SQL("""
            CREATE TABLE {schema}.{table} (
                id SERIAL PRIMARY KEY, -- Auto-incrementing primary key
                rank INTEGER,
                bank_name VARCHAR(255),
                market_cap_usd_billion NUMERIC(20, 2), -- Use NUMERIC for currency
                market_cap_eur_billion NUMERIC(20, 2),
                market_cap_gbp_billion NUMERIC(20, 2),
                market_cap_inr_billion NUMERIC(20, 2)
            );
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )

        # Drop table if it exists to replace data (or use IF NOT EXISTS and handle duplicates)
        logger.info(f"Dropping existing table '{schema_name}.{table_name}' if it exists...")
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {schema}.{table};").format(
            schema=sql.Identifier(schema_name), table=sql.Identifier(table_name)))
        logger.info(f"Existing table '{schema_name}.{table_name}' dropped.")

        # Create the table
        logger.info(f"Creating table '{schema_name}.{table_name}'...")
        cursor.execute(create_table_sql)
        logger.info(f"Table '{schema_name}.{table_name}' created.")

        # Prepare data for insertion
        # We need to convert DataFrame rows to a list of tuples
        data_to_insert = [tuple(row) for row in df[['Rank', 'Bank name', 'Market cap (US$ billion)',
                                                    'Market cap (EUR billion)', 'Market cap (GBP billion)',
                                                    'Market cap (INR billion)']].values]

        # Define the INSERT statement
        # We exclude the 'id' column as it's SERIAL and auto-generated
        insert_sql = sql.SQL("""
            INSERT INTO {schema}.{table} (rank, bank_name, market_cap_usd_billion, market_cap_eur_billion, market_cap_gbp_billion, market_cap_inr_billion)
            VALUES (%s, %s, %s, %s, %s, %s);
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )

        # Use executemany for efficient bulk insertion
        if data_to_insert:
            logger.info(f"Inserting {len(data_to_insert)} rows into '{schema_name}.{table_name}'...")
            cursor.executemany(insert_sql, data_to_insert)
            logger.info(f"Successfully inserted {len(data_to_insert)} rows.")
        else:
            logger.warning("No data rows to insert.")


        # Commit the transaction
        conn.commit()
        logger.info("\nData loading transaction committed successfully.")

    except psycopg2.OperationalError as e:
        logger.error(f"Operational error during database loading: {e}")
        logger.error("Please check database connection parameters, user privileges, and ensure the database is accessible.")
        if conn:
             conn.rollback() # Roll back the transaction on error
             logger.error("Transaction rolled back.")
    except psycopg2.ProgrammingError as e:
        logger.error(f"Database programming error during loading: {e}")
        if conn:
             conn.rollback() # Roll back the transaction on error
             logger.error("Transaction rolled back.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during database loading: {e}")
        if conn:
             conn.rollback() # Roll back the transaction on error
             logger.error("Transaction rolled back.")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed.")


# --- GUI Application Class ---

class BankProcessorGUI(tk.Tk):
    def _init_(self):
        super()._init_()

        self.title("Bank Data Processor (PostgreSQL)") # Updated title
        self.geometry("800x600")

        # Configure grid column to expand
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1) # Row for the log area

        # --- Widgets ---
        self.control_frame = tk.Frame(self)
        self.control_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)

        self.scrape_button = tk.Button(self.control_frame, text="Scrape & Process Data", command=self.on_scrape_button_click)
        self.scrape_button.pack(side=tk.LEFT, padx=5)

        # Add a button to load data to database
        self.load_db_button = tk.Button(self.control_frame, text="Load to PostgreSQL DB", command=self.on_load_db_button_click) # Updated button text
        self.load_db_button.pack(side=tk.LEFT, padx=5)
        self.load_db_button.config(state='disabled') # Disable until data is processed

        self.log_text = scrolledtext.ScrolledText(self, wrap=tk.WORD, state='disabled')
        self.log_text.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)

        # --- Internal State ---
        self.processed_data = None # To hold the DataFrame after scraping and transformation

        # --- Set up GUI Logging Handler ---
        self.gui_handler = TextWidgetHandler(self.log_text)
        self.gui_handler.setLevel(logging.INFO) # Log INFO and above to GUI
        self.gui_handler.setFormatter(formatter) # Use the same formatter
        logger.addHandler(self.gui_handler)

        # Initial log message
        logger.info("GUI started. Ready to process bank data for PostgreSQL.")

    def on_scrape_button_click(self):
        """Handles the button click for scraping and processing."""
        logger.info("-" * 30)
        logger.info("Starting data scraping and processing...")
        self.processed_data = None # Clear previous data
        self.load_db_button.config(state='disabled') # Disable load button

        # Step 1: Scrape data
        df_scraped = scrape_bank_data(URL)

        if df_scraped is not None:
            # Step 2: Transform data
            self.processed_data = transform_data(df_scraped, EXCHANGE_RATES)

            if self.processed_data is not None:
                logger.info("Data processing complete.")
                self.load_db_button.config(state='normal') # Enable load button
            else:
                 logger.error("Data transformation failed.")
        else:
            logger.error("Data scraping failed.")

        logger.info("-" * 30)


    def on_load_db_button_click(self):
        """Handles the button click for loading data to the database."""
        if self.processed_data is not None:
            logger.info("-" * 30)
            logger.info("Starting data loading to PostgreSQL database...")
            load_to_database_psycopg2(self.processed_data, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, SCHEMA_NAME, TABLE_NAME)
            logger.info("Database loading process finished.")
            logger.info("-" * 30)
        else:
            logger.warning("No processed data available to load to the database. Please scrape and process first.")

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure psycopg2 is installed
    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 library not found.")
        print("Please install it using: pip install psycopg2-binary")
        sys.exit(1)


    app = BankProcessorGUI()
    app.mainloop()

    # Clean up file handler when the GUI closes
    logger.removeHandler(file_handler)
    file_handler.close()
