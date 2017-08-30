docstr = """
Sequelize CSV
Usage: sequelize_csv.py [-h]

Options:
  -h --help                                     show this message and exit
Instructions:
  Instructions
"""

import csv
import sqlite3
import os

from docopt import docopt

def main(args):
    print "Inside main"
    database = "redi.db"
    source = "."
    tablesCreated = set()
    # Create database connection
    conn = create_connection(database)
    if conn is not None:
        # Find all files and add in sql
        for root, dirnames, filenames in os.walk(source):
            for filename in filenames:
                if filename.endswith('.txt'):
                    print "filename %s" % filename
                    print "root %s" % root
                    tableName = root.rpartition("/")[-1]
                    print tableName
                    filepath = os.path.join(root, filename)
                    print filepath
                    with open(filepath, "rb") as f:
                        reader = csv.reader(f)
                        cols = reader.next();
                        cols = ["date"] + cols
                        if tableName not in tablesCreated:
                            print "creating table"
                            tablesCreated.add(tableName)
                            create_table(conn, tableName, cols)

                        fileData = [row for row in reader]

    else:
        print("Error! cannot create the database connection.")

def create_table(conn, tableName, cols):
    """create a table in sqlite
    :param db_file: database file
    """

    create_table_sql = "create table if not exists %s (" % tableName
    for column in cols:
        create_table_sql = create_table_sql + column + " text "
    create_table_sql += ")"
    print "create table query %s" % create_table_sql
    execute_sql(conn, create_table_sql)

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

def execute_sql(conn, sql_string):
    """ Execute a SQL statement
    :param conn: Connection object
    :param sql_string: a SQL String
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql_string)
    except sqlite3.Error as e:
        print(e)

def cli_run():
    args = docopt(docstr)
    main(args)

if __name__ == '__main__':
    cli_run()
    exit()
