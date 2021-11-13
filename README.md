# Airflow
Scripts of Airflow

1.) Alert_Mail.py<br/>

   -> It can be used in the case when we want to monitor some table values and if it become less/more than the expected threshold value and we need to notify people over mail.<br/>
   -> 3 airflow operators used for the script<br/>
     1.) MySqlHook: https://airflow.apache.org/docs/apache-airflow-providers-mysql/1.0.0/_api/airflow/providers/mysql/hooks/mysql/index.html<br/>
     2.) PythonOperator: https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html<br/>
     3.) send_email_smtp: https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html <br/>
