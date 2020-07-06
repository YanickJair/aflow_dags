from airflow.exceptions import AirflowSkipException
import csv

"""
Write the query result to a CSV file.
If no file name were provided, create a new one and push to the next task.

:param data_from_sql_query_string
:param sql_query_columns
:param csv_filename

:return filename (optional)
"""
def write_to_csv(**kwargs):
    try:
        ti          = kwargs['ti']
        DATA        = ti.xcom_pull(key="data_from_sql_query_string")
        COLUMNS     = ti.xcom_pull(key="sql_query_columns")
        FILENAME    = ti.xcom_pull(key="csv_filename")

        if not DATA or not COLUMNS:
            raise AirflowSkipException("No data available.")

        if not FILENAME:
            FILENAME = str(datetime.now()) + ".csv"
            ti.xcom_push(key="last_created_filename", value=filename)

        with open("csvs/" + FILENAME, mode="w") as users_file:
            user_write = csv.writer(users_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            user_write.writerow(COLUMNS)
            user_write.writerows(DATA)
    except:
        raise
