import yagmail
import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.mysql.hooks.mysql import MySqlHook

class QueryToMail(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            mysql_conn_id: str,
            sql_query: str,
            archive_name: str,
            file_name: str,
            body: str,
            subject: str,
            receiver: list,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql_query = sql_query
        self.archive_name = archive_name
        self.file_name = file_name
        self.body = body
        self.subject = subject
        self.receiver = receiver

    def execute(self, context):
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        data = hook.get_pandas_df(self.sql_query)
        compression_opts = dict(method='zip',archive_name=self.archive_name)
        data.to_csv(self.file_name, compression=compression_opts, index=False)
        yag = yagmail.SMTP({"sample@gmail.in":"Data Team"},"Password")
        yag.send(to=self.receiver,subject=self.subject,contents=self.body,attachments=self.file_name)
        logging.info('Email Sent Successfully To = %s',self.receiver)
