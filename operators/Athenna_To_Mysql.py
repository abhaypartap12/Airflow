import logging
import awswrangler as wr
from typing import Optional,Any
from pangres.core import upsert
from sqlalchemy import create_engine
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class AthennaToMySqlOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            *,
            sql: str,
            mysql_table: str,
            aws_conn_id: Optional[str] = 'datalake',
            client_type: Optional[str] = 's3',
            region_name: Optional[str] = 'ap-south-1',
            database: str,
            if_row_exists: str,
            index_col_name: str,
        **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.client_type = client_type
        self.region_name = region_name
        self.aws_conn_id = aws_conn_id
        self.database = database
        self.if_row_exists = if_row_exists
        self.index_col_name = index_col_name

    def execute(self, context):
        hook=AwsBaseHook(aws_conn_id=self.aws_conn_id ,client_type=self.client_type)
        session=hook.get_session(region_name=self.region_name)
        logging.info("Extracting data from Athenna: %s", self.sql)
        data=wr.athena.read_sql_query(sql=self.sql,database=self.database,boto3_session=session)

        data.set_index(self.index_col_name, inplace = True)
        engine = create_engine("mysql+pymysql://user:passwd@hostname/databasename",connect_args= dict(host='hostname', port=3306))
        upsert(engine=engine,df=data,table_name=self.mysql_table,if_row_exists=self.if_row_exists)
        logging.info('Shape Of DataFrame Upsert Is = %s',data.shape)
