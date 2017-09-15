docstr = """
Sequelize CSV

Usage:
    sequelize_csv.py <root_path> [-h][-d <db_file>]
    sequelize_csv.py report <sql_file>[-a][-d <db_file>]

Options:
  -h --help                                     show this message and exit
  -d <db_file> --database <db_file>             database file
  -a --all                                      run against all sites
"""

import csv
import sqlite3
import os

from docopt import docopt

import query_processor
import report_helper


def main(args):

    # Create database connection
    if args.get("--database"):
        database = args.get("--database")
    else:
        database = "redi.db"
    conn = create_connection(database)

    if args.get("report"):
        # Run sql query
        if args.get("<sql_file>"):
            report_helper.execute_sql(conn, args.get("<sql_file>"), args.get("--all"))
            return

    else:
        # Sequelize CSVs
        if args.get("<root_path>"):
            root = args.get("<root_path>")
            root = root.rstrip("/")
        else:
            print "Invalid path of root directory."
            return

        sequelize(conn, root)


def sequelize(conn, root):
    """
    Store data from CSVs to MySQL tables
    :param conn: database connection
    :param root: path of the root directory
    :return:
    """
    if conn is not None:
        query_processor.create_files_processed_table(conn)

        # Find all files and add in sql
        for root, dirnames, filenames in os.walk(root):
            for filename in filenames:
                if filename.endswith(('.txt', '.csv')):
                    parent_dir = root.rpartition("/")[-1]
                    if query_processor.is_new_file(conn, parent_dir, filename):
                        print "Processing file: %s in directory: %s" % (filename, root)
                        file_path = os.path.join(root, filename)
                        with open(file_path, "rb") as f:
                            reader = csv.reader(f)
                            cols = reader.next()
                            cols = ["date"] + cols
                            print "Creating table %s" % parent_dir
                            query_processor.create_table(conn, parent_dir, cols)

                            date = filename.split(".", 1)[0]
                            file_data = [(date,) + tuple(row) for row in reader]
                            query_processor.insert_data_in_table(conn, parent_dir, cols, file_data)

    else:
        print("Error! cannot create the database connection.")
    print "All files processed!"


def create_connection(db_file):
    """
    Create a database connection to the SQLite database specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

    return None


def cli_run():
    args = docopt(docstr)
    main(args)


if __name__ == '__main__':
    cli_run()
    exit()
