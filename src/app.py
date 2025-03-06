import os
from simpleETL_coded_noSSIS import etl_noSSIS

def main():
    print("Starting ETL process...")
    etl_noSSIS()
    print("ETL process completed")

if __name__ == "__main__":
    main()