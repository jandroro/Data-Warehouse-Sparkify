"""
Script used to connect us to Redshift cluster, delete previous tables and
create staging, fact and dimensional tables newly.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Method used to drop tables using the queries
    located in 'drop_table_queries' list.
    
    Args:
        cur: Cursor created as part of the redshift connection.
        conn: Redshift connection instance.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Method used to create tables using the queries
    located in 'create_table_queries' list.
    
    Args:
        cur: Cursor created as part of the redshift connection.
        conn: Redshift connection instance.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Method used to establish a redshift connection, execute both
    the 'drop_tables' and 'create_tables' methods.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()