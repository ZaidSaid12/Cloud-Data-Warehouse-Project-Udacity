import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Description: Drop the current tables existing in the database
    Parameters:
        - cur: Cursor which is used in executing query on the database.
        - conn: Connection to the database
    Returns:
        - None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Description: Create tables on the database.
    Parameters:
        - cur: Cursor which is used in executing query on the database.
        - conn: Connection to the database
    Returns:
        - None
    '''
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()