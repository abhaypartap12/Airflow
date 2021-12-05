import pendulum
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.query_to_mail import QueryToMail

local_tz = pendulum.timezone("Asia/Kolkata")

args = {
    'owner': 'Abhay',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 5, tzinfo=local_tz),
    'email' : ['abc@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='Custom_Operator',default_args=args,schedule_interval='00 9,12,15 * * 1-7')

query= open('/root/airflow/sql/file.sql', 'r').read()
body = "Hi,<br><br>PFA<br><br>Regards,<br>Abhay"
subject = 'file_name-' + str(datetime.now().strftime('%d %B %Y %I %p'))
archive_name = 'file_name'+ str(datetime.now().strftime('%Y%m%d-%I%p'))+'.csv'
file_name = "/root/airflow/data/file/file_name"+ str(datetime.now().strftime('%Y%m%d-%I%p'))+".zip"
receiver = ["abc@gmail.com","xyz@gmail.com","qwe@gmail.com"]

QueryToMail = QueryToMail(task_id='Send_Report',mysql_conn_id='connection_name',sql_query=query,body=body,subject=subject,archive_name=archive_name,file_name=file_name,receiver=receiver,dag=dag)
