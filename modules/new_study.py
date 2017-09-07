def start_up():
    print "Start Up!"


def sql_run(conn):
    query = "SELECT * FROM sqlite_master WHERE type='table' and tbl_name like 'site%'"
    c = conn.cursor()
    c.execute(query)
    sites = c.fetchall()
    for site in sites:
        select_all_query = "SELECT * FROM %s ORDER BY DATE(date)" % site[2]
        c.execute(select_all_query)
        rows = c.fetchall()
        all_studies = set()
        daily_studies = set()
        curr_date = rows[0][0]
        for row in rows:
            if row[0] == curr_date:
                daily_studies.add(row[1])

            else:
                # If first day add all reported studies to superset
                if len(all_studies) == 0:
                    all_studies = all_studies.union(daily_studies)

                else:
                    if not all_studies.issuperset(daily_studies):
                        report_new_studies(all_studies, daily_studies, curr_date)
                        all_studies = all_studies.union(daily_studies)

                curr_date = row[0]
                daily_studies = set()

        if len(all_studies) > 0 and not all_studies.issuperset(daily_studies):
            report_new_studies(all_studies, daily_studies, curr_date)


def report():
    print "Report!"


def report_new_studies(all_studies, daily_studies, curr_date):

            new_studies = all_studies - daily_studies
            new_studies_string = ",".join(new_studies)
            print "New studies reported on %s are %s" % (curr_date, new_studies_string)
