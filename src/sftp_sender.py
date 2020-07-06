import pysftp

from omegaconf import OmegaConf

from airflow.models import Variable

conf = OmegaConf.load('config.yaml')
sftp_conn = Variable.get("local_sftp_conn", deserialize_json=True)

def send_cvs_file_via_sftp(**kwargs):
    try:
        ti          = kwargs['ti']
        FILENAME    = ti.xcom_pull(key="csv_filename")

        if not FILENAME:
            FILENAME = ti.xcom_pull(key="last_created_filename")

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        with pysftp.Connection(sftp_conn["host"], username=sftp_conn["user"], password=conf.sftp.password, cnopts=cnopts) as sftp:
            with sftp.cd('DIR'):
                sftp.put("/destination/dir/name/" + FILENAME)
    except:
        raise