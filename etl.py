"""
Script used to connect us to Redshift cluster, load data into
staging, fact and dimensional tables.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Method used to load data into staging tables using
    the queries located in 'copy_table_queries' list.
    
    Args:
        cur: Cursor created as part of the redshift connection.
        conn: Redshift connection instance.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Method used to load data into fact and dimensional tables
    using the queries located in 'insert_table_queries' list.
    
    Args:
        cur: Cursor created as part of the redshift connection.
        conn: Redshift connection instance.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Method used to establish a redshift connection, execute both
    the 'load_staging_tables' and 'insert_tables' methods to
    populate our redshift cluster.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()