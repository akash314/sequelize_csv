from sqlite3 import OperationalError


def execute_sql(conn, sql_file_path, all_sites=False):
    """
    Run sql commands in file
    :param conn: database connection
    :param sql_file_path: path of the sql file
    :param all_sites: True if queries has to be executed across all sites
    :return
    """
    # Open and read the file as a single buffer
    fd = open(sql_file_path, 'r')
    sql_file = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sql_commands = sql_file.split(';')

    if all_sites:
        run_across_all_sites(conn, sql_commands)
    else:
        run_commands(conn, sql_commands)


def run_across_all_sites(conn, sql_commands):
    """
    Run sql commands across all sites
    :param conn: database connection
    :param sql_commands: list of sql queries to execute
    :return
    """
    get_sites_query = "SELECT * FROM sqlite_master WHERE type='table' and tbl_name like 'site%'"
    c = conn.cursor()
    c.execute(get_sites_query)
    rows = c.fetchall()

    for row in rows:
        for command in sql_commands:
            # This will skip and report errors
            try:
                final_query = command.replace('%table%', row[2])
                print final_query
                c.execute(final_query)
                result = c.fetchall()
                for result_row in result:
                    print result_row

            except OperationalError, msg:
                print "Command skipped: ", msg


def run_commands(conn, sql_commands):
    """
    Run sql commands
    :param conn: database connection
    :param sql_commands: list of sql queries to execute
    :return
    """

    # Execute every command from the input file
    for command in sql_commands:
        # This will skip and report errors
        try:
            print command
            c = conn.cursor()
            c.execute(command)
            result = c.fetchall()
            for row in result:
                print row

        except OperationalError, msg:
            print "Command skipped: ", msg



