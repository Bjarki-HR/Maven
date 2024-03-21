import pyodbc
from vars import db_server
import pandas as pd

class Db_Session:
    def __init__(self, conn_str) -> None:
        self.cnxn = pyodbc.connect(conn_str, autocommit=False)
        self.cursor = self.cnxn.cursor()
        print(f"Connected to DB: {db_server.split(',')[0]}")

    def execute_select(self):
        self.cursor.execute('Insert into test (id) values(10)')
        self.cursor.commit()
    
    def custom_sql_query(self, query):
        self.cursor.execute(query)
        self.cursor.commit()
    
    def fetch_dataframe(self, query):
        """
        Executes a SQL query and returns the result as a Pandas DataFrame.
        """
        return pd.read_sql_query(query, self.cnxn)
    
    def update_value_multiplier(self, table_name):
        update_query = f"""
        UPDATE {table_name}
        SET Value = Value * 1000
        WHERE YearMonth = '2013 Mars'
        """
        try:
            self.cursor.execute(update_query)
            self.cnxn.commit()
            print("Values updated for '2013 Mars' successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
            self.cnxn.rollback()  # Rollback in case of error

    def insert_dataframe(self, df, table_name):
        for index, row in df.iterrows():
            # Get column names and values from the row
            columns = row.index
            values = row.values

            # Create a list of formatted column names
            formatted_columns = [f"[{column}]" for column in columns]

            # Create a list of formatted values
            formatted_values = []
            for value in values:
                if isinstance(value, str):
                    value = value.replace("'", "''")  # Escape single quotes in string values
                    formatted_values.append(f"'{value}'")
                elif pd.isna(value):
                    formatted_values.append('NULL')  # Handle NaN values explicitly
                elif isinstance(value, pd.Timestamp):
                    formatted_values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")  # Format datetime values
                else:
                    formatted_values.append(str(value))

            # Construct the SQL INSERT statement
            columns_str = ', '.join(formatted_columns)
            values_str = ', '.join(formatted_values)
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"

            # Execute the query
            self.custom_sql_query(sql)

        print(f"Data inserted into {table_name}")
        
    def create_table_from_df(self, df, table_name):
        create_statement = f"CREATE TABLE {table_name} ("

        columns = []
        for column in df.columns:
            dtype = df[column].dtype
            sql_type = "FLOAT" if "float" in str(dtype) else "INT" if "int" in str(dtype) else "VARCHAR(255)"
            # Enclose column names in square brackets
            formatted_column = f"[{column}] {sql_type}"
            columns.append(formatted_column)

        create_statement += ', '.join(columns)
        create_statement += ")"

        # Execute the query
        self.custom_sql_query(create_statement)
        print(f"Table '{table_name}' created.")
    
    def check_table_exists(self, table_name):
        # Query to check if the table exists
        check_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}'"
        self.cursor.execute(check_query)
        return bool(self.cursor.fetchone())

    def terminate_connection(self):
        print("Close Connection")
        self.cursor.close()
        self.cnxn.close()
        print("Done")
    