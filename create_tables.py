# sql_queries.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 01/10/2020
# PURPOSE: Script to initialize environment by creating database and tables for Data Engineering Project 1a.
#
# Included functions:
#     create_database    - create and initialize development database
#     drop_tables        - drop DWH fact and dimension tables
#     create_tables      - create DWH fact and dimension tables
#     main               - main function performs database initialization 
# 

import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    create and initialize database
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to new sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    drop DWH fact and dimension tables
    """
    
    for table, query in drop_table_queries.items():
        try:
            cur.execute(query)
            conn.commit()
            print(f"Drop table command succeeded for table '{table}'.") 
        except psycopg2.Error as e:
            print(f"Drop table command failed for table '{table}'.")
            print(e)
            conn.rollback()
            continue


def create_tables(cur, conn):
    """
    create DWH fact and dimension tables
    """
    
    for table, query in create_table_queries.items():
        try:
            cur.execute(query)
            conn.commit()
            print(f"Create table command succeeded for table '{table}'.") 
        except psycopg2.Error as e:
            print(f"Create table command failed for table '{table}'.")
            print(e)
            conn.rollback()
            continue


def main():
    """
    main function performs database initialization
    Parameters: none
    """
    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()