import sqlite3


def execute_single_sql(conn, sql_string):
    """
    Execute a single SQL statement
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
    """
    Execute a SQL statement
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
