def generate_reports(module):
    """
    Checks if the file is processed yet
    :param module: python module
    :return:
    """
    report_module = __import__(module)
    report_module.start_up()
    report_module.sql_run()
    report_module.report()

