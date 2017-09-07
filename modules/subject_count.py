def start_up():
    print "Start Up!"


def sql_run(conn):
    query = "SELECT * FROM sqlite_master WHERE type='table' and tbl_name like 'site%'"
    c = conn.cursor()
    c.execute(query)
    rows = c.fetchall()
    for row in rows:
        count_subs_query = "SELECT COUNT(DISTINCT STUDY_ID) FROM %s" % row[2]
        c.execute(count_subs_query)
        count = c.fetchone()
        print "Site: %s has a subject count of %s" % (row[2], count[0])


def report():
    print "Report!"


