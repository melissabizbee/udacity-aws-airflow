from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Airflow operator for loading data into the fact table.

    Args:
        redshift_conn_id (str): The Redshift Airflow Conn Id.
        sql (str): The SQL code to execute.
    """
     
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        self.log.info(f'Insert data into fact table: {self.table}')
        redshift = PostgresHook(self.redshift_conn_id)        
        redshift.run(self.sql)