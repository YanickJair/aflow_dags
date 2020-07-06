from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowSkipException

"""
Query data using a given sql query string.

:param sql_query
:param DB Connector

:return query result
"""
def query_data(**kwargs):
    try:
        ti          = kwargs['ti']
        SQL_QUERY   = ti.xcom_pull(key="sql_query_string")
        CONNECTOR   = ti.xcom_pull(key="db_connector")

        if not SQL_QUERY or not CONNECTOR:
            raise AirflowSkipException("Could not query because Query SQL or Connector ID are missing.")

        pg_hook = PostgresHook(
            postgres_conn_id=CONNECTOR,
        ).get_conn()

        with pg_hook.cursor("serverCursor") as pg_cursor:
            pg_cursor.execute(SQL_QUERY)
            rows = pg_cursor.fetchall()
            users = [list(row) for row in rows]
        ti.xcom_push(key="data_from_sql_query_string", value=users)
    except:
        raise
