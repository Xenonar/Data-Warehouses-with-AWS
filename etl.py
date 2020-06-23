import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Description: This is the fuction for LOAD dataset into stage tables
    
    Arguments:
        cur: the cursor object. 
        conn: for connection to database inside AWS Redshift cluster 
        
    Returns:
        None
    
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Description: This is the fuction for INSERT data from stage tables into fact and dimensional tables
    
    Arguments:
        cur: the cursor object. 
        conn: for connection to database inside AWS Redshift cluster 
        
    Returns:
        None
    
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Description: This is the main fuction for made connection to Redshift cluster and load data into stage tables then insert the data

    Arguments:
        None
        
    Returns:
        None
    
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