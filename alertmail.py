import airflow
import pendulum
from airflow import DAG
from datetime import datetime,date
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email_smtp
from airflow.providers.mysql.hooks.mysql import MySqlHook

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'abhay',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 9, tzinfo=local_tz),
    'email' : ['abhay.partap12@gmail.com'],
    'email_on_failure': True
}

dag = DAG('Alert_Mail',default_args=default_args,schedule_interval= '*/30 8-22 * * *')


def alert_task():
    platform_mysql_read_hook = MySqlHook(mysql_conn_id='connection_id_name')
    sql_query="SELECT count(1) AS cnt FROM table WHERE create_date >=DATE_SUB(NOW(),INTERVAL 30 MINUTE);"
    records = platform_mysql_read_hook.get_pandas_df(sql_query)
    if records['cnt'][0] < 10:
        subject = "Alert: Count Value is low!!"
        html_content = """Hi All,<br><br>
Please check the count value is less than 10 for last 30 Mints.<br><br>
Regards,<br>
Abhay"""
        to = 'abc@gmail.com'
        cc = ['xyz@gmail.com','pqr@gmail.com']
        send_email_smtp(to, subject, html_content,None,False, cc, bcc=None, mime_subtype='mixed', mime_charset='us-ascii')

task = PythonOperator(dag=dag,task_id='alert_task',python_callable=alert_task)

task
