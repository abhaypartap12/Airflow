# Airflow
### Scripts of Airflow

1.) [dags] Alert_Mail.py - send alert email using airflow<br/>

   - It can be used in the case when we want to monitor some table values and if it become less/more than the expected threshold value and we need to notify people over mail.<br/>
   - 3 airflow operators used for the script<br/>
       1.) MySqlHook: [Link](https://airflow.apache.org/docs/apache-airflow-providers-mysql/1.0.0/_api/airflow/providers/mysql/hooks/mysql/index.html)<br/>
       2.) PythonOperator: [Link](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html)<br/>
       3.) send_email_smtp: [Link](https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html)<br/>

2.) [operators] Athenna_To_Mysql.py - create pipeline from athenna to Mysql<br/>

   - This can be used as a customized airflow operator to transfer data from athenna to mysql<br/>
   - External Libraries:<br/>
       1.) upsert: [Link](https://pypi.org/project/upsert/)<br/>
       2.) create_engine: [Link](https://docs.sqlalchemy.org/en/14/core/engines.html)<br/>
       3.) logging: [Link](https://docs.python.org/3/howto/logging.html)<br/>
   - AthennaToMySqlOperator class variables:<br/>
      1.) sql: Query to fetch data from S3 using Athenna Query Engine (source data)<br/>
      2.) mysql_table: Destination Mysql table name<br/>
      3.) aws_conn_id: [Airflow Connections AWS](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html)<br/>
      4.) client_type: 's3'<br/>
      5.) region_name: AWS region for the connection<br/>
      6.) database: Destination database name<br/>
      7.) if_row_exists: 'update' or 'replace'<br/>
      8.) index_col_name: index column name<br/>
   - Sample Dag File Using this Operator: [Link](https://github.com/abhaypartap12/Airflow/blob/main/dags/ETL_athenna_to_sql.py)<br/>

3.) [operators] Mysql_query_to_Email.py - create pipeline to extract data from Mysql and send to Email as Attachment<br/>

   - This can be used as a customized airflow operator to extract data from Mysql and send to Email as Attachment<br/>
   - External Libraries:<br/>
       1.) yagmail: [Link](https://github.com/abhaypartap12/python-mini-projects/tree/send_email/projects/Send_Email)<br/>
       2.) logging: [Link](https://docs.python.org/3/howto/logging.html)<br/>
   - QueryToMail class variables:<br/>
     1.) mysql_conn_id: [Link](https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/connections/mysql.html)<br/>
     2.) sql_query: Path of Query to fetch data from Mysql DB<br/>
     3.) archive_name: File name to be stored Inside Zip Folder<br/>
     4.) file_name: Zip file name to attach in Mail<br/>
     5.) body: Email body to be sent<br/>
     6.) subject: Email subject<br/>
     7.) receiver: List of Receiver's Email Id<br/>
   - Sample Dag File Using this Operator: [Link](https://github.com/abhaypartap12/Airflow/blob/main/dags/ETL_mysql_to_mail.py)<br/>
