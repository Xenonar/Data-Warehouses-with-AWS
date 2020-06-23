import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Description: This is the fuction for DROP tables before create new table
    
    Arguments:
        cur: the cursor object. 
        conn: for connection to database inside AWS Redshift cluster 
        
    Returns:
        None
    
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Description: This is the fuction for CREATE tables 
    
    Arguments:
        cur: the cursor object. 
        conn: for connection to database inside AWS Redshift cluster 
        
    Returns:
        None
    
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Description: This is the main fuction for execute DROP table and CREATE new table

    Arguments:
        None
        
    Returns:
        None
    
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