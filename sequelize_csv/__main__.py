docstr = """
Sequelize CSV

Usage: sequelize_csv.py [-h][-r <root dir>]

Options:
  -h --help                                     show this message and exit
  -r <root dir> --root <root dir>               root directory

"""

import csv
import sqlite3
import os

from docopt import docopt

def main(args):
    if args.get("--root"):
        root = args.get("--root")
    else:
        root = "."
    database = "redi.db"

    tables_created = set()
    # Create database connection
    conn = create_connection(database)
    if conn is not None:
        # Find all files and add in sql
        for root, dirnames, filenames in os.walk(root):
            for filename in filenames:
                if filename.endswith(('.txt', '.csv')):
                    table_name = root.rpartition("/")[-1]
                    print "Processing file: %s in directory: %s" % (filename, root)
                    file_path = os.path.join(root, filename)
                    with open(file_path, "rb") as f:
                        reader = csv.reader(f)
                        cols = reader.next()
                        cols = ["date"] + cols
                        if table_name not in tables_created:
                            print "Creating table %s" % table_name
                            tables_created.add(table_name)
                            create_table(conn, table_name, cols)

                        date = filename.split(".",1)[0]
                        file_data = [(date,) + tuple(row) for row in reader]
                        insert_data_in_table(conn, table_name, cols, file_data)

    else:
        print("Error! cannot create the database connection.")


def insert_data_in_table(conn, table_name, cols, data):
    """write data from csv to sqlite
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
    execute_many_sql(conn, insert_table_query, data)


def create_table(conn, tableName, cols):
    """create a table in sqlite
    :param conn: database connection
    :param tableName: table name
    :param cols: table columns
    """

    create_table_query = "CREATE TABLE IF NOT EXISTS %s (" % tableName
    for column in cols:
        create_table_query += "%s text," % column
    create_table_query = create_table_query[:-1]
    create_table_query += ")"
    execute_single_sql(conn, create_table_query)

    unique_key = ",".join(cols)
    unique_index_query = "CREATE UNIQUE INDEX %s_index ON %s (%s)" % (tableName, tableName, unique_key)
    print unique_index_query
    execute_single_sql(conn, unique_index_query)


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

    return None


def execute_single_sql(conn, sql_string):
    """ Execute a single SQL statement
    :param conn: Connection object
    :param sql_string: a SQL String
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql_string)
    except sqlite3.Error as e:
        print(e)


def execute_many_sql(conn, sql_string, data):
    """ Execute a SQL statement
    :param conn: Connection object
    :param sql_string: a SQL String
    :param data:
    :return:
    """
    try:
        c = conn.cursor()
        c.executemany(sql_string, data)
        conn.commit()
    except sqlite3.Error as e:
        print(e)


def cli_run():
    args = docopt(docstr)
    main(args)


if __name__ == '__main__':
    cli_run()
    exit()
