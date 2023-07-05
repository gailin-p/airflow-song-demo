from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    Load a fact table from a staging table using a redshift query. 
    User passes a SQL statement. 
    Optionally truncate and recreate table. 
    """

    ui_color = '#F98866'
    drop_template = SqlQueries.table_drop

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 conn_id = "aws_credentials",
                 sql_load_statement = "", 
                 sql_create_statement = "",
                 table_name = "",
                 truncate=False, # Because fact tables are large, we usually don't want to truncate 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id 
        self.truncate = truncate 
        self.sql_create = sql_create_statement
        self.sql_load = f"INSERT INTO {table_name} {sql_load_statement}"
        self.table_name = table_name

        # Ensure user overwrote defaults 
        assert len(sql_load_statement) > 0 

    def execute(self, context):
        redshift = PostgresHook(self.conn_id)
        if self.truncate: 
            del_table = self.drop_template.format(self.table_name)
            redshift.run(del_table)
            redshift.run(self.sql_create)
            redshift.run(self.sql_load)
        else: 
            redshift.run(self.sql_load)

