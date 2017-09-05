import sqlite_utils
from datetime import datetime


def create_files_processed_table(conn):
    """
    Create files_processed table if not created already
    :param conn: database connection
    :return:
    """
    create_query = "CREATE TABLE IF NOT EXISTS files_processed (file_path text, date text)"
    sqlite_utils.execute_single_sql(conn, create_query)


def is_new_file(conn, parent_dir, filename):
    """
    Checks if the file is processed yet
    :param conn: database connection
    :param parent_dir: File parent directory
    :param filename: File name
    :return: True if the file is new, otherwise False
    """
    file_path = "*/%s/%s" % (parent_dir, filename)
    file_processed_query = "SELECT * from files_processed where file_path=?"
    c = conn.cursor()
    c.execute(file_processed_query, (file_path,))
    row = c.fetchone()
    if row is None:
        insert_row_query = "INSERT into files_processed (file_path, date) VALUES (?, ?)"
        c.execute(insert_row_query, (file_path, datetime.utcnow()))
        conn.commit()
        return True
    else:
        return False


def insert_data_in_table(conn, table_name, cols, data):
    """
    Write data from csv to sqlite
    :param conn: database connection
    :param table_name: table name
    :param cols: table columns
    :param data: table data
    """
    insert_table_query = "INSERT OR IGNORE into %s(" % table_name
    for column in cols:
        insert_table_query += ("%s," % column)

    insert_table_query = insert_table_query[:-1]
    insert_table_query += ") VALUES(%s" % ("?," * len(cols))
    insert_table_query = insert_table_query[:-1]
    insert_table_query += ")"
    sqlite_utils.execute_many_sql(conn, insert_table_query, data)


def create_table(conn, table_name, cols):
    """
    Create a table in sqlite
    :param conn: database connection
    :param table_name: table name
    :param cols: table columns
    """

    create_table_query = "CREATE TABLE IF NOT EXISTS %s (" % table_name
    for column in cols:
        create_table_query += "%s text," % column
    create_table_query = create_table_query[:-1]
    create_table_query += ")"
    sqlite_utils.execute_single_sql(conn, create_table_query)

    unique_key = ",".join(cols)
    unique_index_query = "CREATE UNIQUE INDEX %s_index ON %s (%s)" % (table_name, table_name, unique_key)
    print unique_index_query
    sqlite_utils.execute_single_sql(conn, unique_index_query)
