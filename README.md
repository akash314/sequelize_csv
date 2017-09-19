# Sequelize CSV

Imports CSV files into SQLite.

The directory which will serve as the root for the import process is expected to have the following structure.

```
root
│
└───site1
│   └───date1.txt
│   └───date2.txt
│
└───site2
    └───date1.txt
    └───date2.txt
    └───date3.txt
```

Then you can start the import with the command:
```
sequelize_csv <path_to_root> -d <database_file_path>
```

To run sql commands against the sqlite3 database having the imported data specify the query in a sql file.  You may store all sql files in the sql directory.
You can execute the sql queries using the command

```
sequelize_csv report <path_to_sql_file> -d <database_file_path>

Eg.:
sequelize_csv report sql/subject_count.sql -d redi.db
```

To run sql commands against all sites use the -a option and in the sql query replace table name with %table_name%

```
sequelize_csv report sql/selectAll.sql -a -d redi.db
```


###### Prerequisites

- Python 2.7
- SQLite 3.9.0 or higher
