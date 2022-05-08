import sqlite3
from sql_queries import create_staging_table_queries, drop_table_queries

def drop_tables(cur, con):
    """
    Drop the tables if existed for initialization.
    INPUT:
        cur: cursor object
        con: connection object
    OUTPUT:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        con.commit()


def create_staging_tables(cur, con):
    """
    Create staging tables based on the sql queries which contain the schema.
    INPUT:
        cur: cursor object
        con: connection object
    OUTPUT:
        None
    """
    for query in create_staging_table_queries:
        cur.execute(query)
        con.commit()


def main():
    """ Assign the parameters and load the functions """
    # create connection and cursor objects
    con = sqlite3.connect('states.db')
    cur = con.cursor()
    
    # run the functions
    drop_tables(cur, con)
    create_staging_tables(cur, con)

    con.close()


if __name__ == "__main__":
    main()