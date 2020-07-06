from airflow.models import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom

@provide_session
def clean_xcom_values(context, session=None):
    try:
        #dag_id = context['ti']['dag_id']
        session.query(XCom).filter(XCom.dag_id == 'sftp_dag').delete()
    except:
        raise