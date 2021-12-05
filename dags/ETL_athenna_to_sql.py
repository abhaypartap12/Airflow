import airflow
import pendulum
from airflow import DAG
from datetime import datetime,timedelta
from airflow.transfers.athenna_to_mysql import AthennaToMySqlOperator

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'Abhay',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 5, tzinfo=local_tz),
    'email' : ['abc@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

today=datetime.now().strftime("%Y%m%d")

with DAG(dag_id='Custom_Operator',default_args=default_args,schedule_interval='*/15 9-23 * * *') as dag:
    sql = ("""( select col1,col2,col3,col4,col5 from table where ptd = '{} )""".format(today))
    mysql_table = "dest_table"
    if_row_exists = "update"
    index_col_name = "id"
    database= 'dest'
    task = AthennaToMySqlOperator(sql=sql,mysql_table=mysql_table,database=database,if_row_exists=if_row_exists,index_col_name=index_col_name,dag=dag,task_id='etl')

    task
