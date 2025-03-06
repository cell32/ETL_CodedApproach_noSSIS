import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import create_engine
from tabulate import tabulate

def etl_noSSIS():
    load_dotenv() # load environment variables

    # df = pd.read_csv(r"C:\Users\SkyNet_1\Documents\product-sales.csv")
    df = pd.read_csv(os.path.join("data", "product-sales.csv"))
    df.info()

    # # Display the DataFrame data format in table format using tabulate
    # print(tabulate(df.head(), headers='keys', tablefmt='grid', showindex=False))

    # Display the DataFrame data format. This is in purpose because Month DD/MM/YYYY
    # is not accepted in mysql which takes YYYY-MM-DD format
    print(df.head(5))

    # Step 1) Transform Month column to datetime format to work with mysql
    df["Month"] = pd.to_datetime(df["Month"], format="%m/%d/%Y")
    print("\nTransformed Month column format")
    print(df.head(5))

    # Step 2) Transform - filter ProductSales column > 200 and < 200
    df_filtered_above200 = df[df["ProductSales"] > 200]
    df_filtered_below200 = df[df["ProductSales"] < 200]

    try:
        # Connect to MySQL server and create database if not exists
        db = "SSISTestDB"
        connection = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("MYSQLUSER"),
            password=os.getenv("MYSQLPASSWORD")
        )

        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        connection.commit()
        print(f"Database {db} created successfully")

        cursor.execute(f"USE {db}")

        # Create the tables
        table_queries = {
            "salesdata_below200": """
            CREATE TABLE IF NOT EXISTS salesdata_below200 (
                Month DATE,
                ProductSales DECIMAL(10,2)
            )
            """,
            "salesdata_above200": """
            CREATE TABLE IF NOT EXISTS salesdata_above200 (
                Month DATE,
                ProductSales DECIMAL(10,2)
            )
            """
        }
    
        for table_name, query in table_queries.items():
            cursor.execute(query)
            print(f"Table '{table_name}' created successfully")

        # Commit changes and close connection
        connection.commit()
        cursor.close()
        connection.close()
        print("Database setup completed successfully.")

    except mysql.connector.Error as err:
        print(f"Error connecting to the database: {err}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

    # Create SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{os.getenv('MYSQLUSER')}:{os.getenv('MYSQLPASSWORD')}@{os.getenv('HOST')}/{db}")

    try:
        # Insert transformed data into MySQL tables
        df_filtered_above200.to_sql("salesdata_above200", con=engine, if_exists='append', index=False)
        print("Data successfully written to salesdata_above200 table")

        df_filtered_below200.to_sql("salesdata_below200", con=engine, if_exists='append', index=False)
        print("Data successfully written to salesdata_below200 table")

    except Exception as ex:
        print(f"Error writing dataframe to MySQL: {ex}")

    finally:
        # Close MySQL connection if open
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")
