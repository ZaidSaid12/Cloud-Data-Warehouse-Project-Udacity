import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Desciption:
        - Uses the copy_table_queries list defined the sql_queries files to copy the data from the s3 bucket provided 
        by udacity and load it into the staging tables created in the amazon redshift cluster
    Parameters:
        - cur: Cursor which is used in executing query on the database.
        - conn: Connection to the database
    Returns:
        - None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Desciption:
        - Uses the insert_table_queries list defined the sql_queries files to insert the data from the staging tables
        into the fact table and dimension tables by applying the required transformations.
    Parameters:
        - cur: Cursor which is used in executing query on the database.
        - conn: Connection to the database
    Returns:
        - None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Description: Main function which is the starting point of the functionality of the code. it gets the required configrations to connect to the cluster using the file dwh.cfg
    Parameters:
        - None
    Returns:
        - None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()