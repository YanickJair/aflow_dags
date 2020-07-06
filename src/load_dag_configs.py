import sys
import json

from airflow.hooks.postgres_hook import PostgresHook

""" 
description load the bytea config attr from dag_run.
The configs are stored using sqlalchemy's PickleType

:param kwargs

:return config object
"""
def load_dag_configs(**kwargs) -> object:
    try:
        ti = kwargs['ti']

        config: object = None

        SQL_QUERY = """ 
            SELECT sql_query, "_columns", db_connector, filename
            FROM public.dag_run_config WHERE dag_id = 2;
        """

        dag_pg_hook = PostgresHook(
            postgres_conn_id="vdfairflow_config",
        ).get_conn()

        with dag_pg_hook.cursor("serverCursor") as cursor:
            keys = ["sql_query_string", "columns", "db_connector", "filename"]
            cursor.execute(SQL_QUERY)
            rows = cursor.fetchall()
            config = [dict(zip(keys, row)) for row in rows]
            
            #* Push configurations to tasks
            ti.xcom_push(key="sql_query_columns", value=config[0]["columns"])
            ti.xcom_push(key="sql_query_string", value=config[0]["sql_query_string"])
            ti.xcom_push(key="db_connector", value=config[0]["db_connector"])
            ti.xcom_push(key="csv_filename", value=config[0]["filename"])
    except:
        raise