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
                if filename.endswith('.txt'):
                    print "filename %s" % filename
                    print "root %s" % root
                    table_name = root.rpartition("/")[-1]
                    print table_name
                    file_path = os.path.join(root, filename)
                    print file_path
                    with open(file_path, "rb") as f:
                        reader = csv.reader(f)
                        cols = reader.next();
                        cols = ["date"] + cols
                        if table_name not in tables_created:
                            print "creating table"
                            tables_created.add(table_name)
                            create_table(conn, table_name, cols)

                        file_data = [(filename,) + tuple(row) for row in reader]
                        print file_data
                        insert_data_in_table(conn, table_name, cols, file_data)

    else:
        print("Error! cannot create the database connection.")


def insert_data_in_table(conn, tablename, cols, data):
    """write data from csv to sqlite
    :param conn: database connection
    :param tableName: table name
    :param cols: table columns
    :param data: table data
    """
    insert_table_sql = "INSERT into %s(" % tablename
    for column in cols:
        insert_table_sql += ("%s," % column)

    insert_table_sql = insert_table_sql[:-1]
    insert_table_sql += ") VALUES(%s" % ("?," * len(cols))
    insert_table_sql = insert_table_sql[:-1]
    insert_table_sql += ")"
    print "insert query" + insert_table_sql
    execute_many_sql(conn, insert_table_sql, data)


def create_table(conn, tableName, cols):
    """create a table in sqlite
    :param conn: database connection
    :param tableName: table name
    :param cols: table columns
    """

    create_table_sql = "CREATE TABLE IF NOT EXISTS %s (" % tableName
    for column in cols:
        create_table_sql += "%s text," % column
    create_table_sql = create_table_sql[:-1]
    create_table_sql += ")"
    print "create table query %s" % create_table_sql
    execute_single_sql(conn, create_table_sql)


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
    print "sql_string" + sql_string
    print data
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
