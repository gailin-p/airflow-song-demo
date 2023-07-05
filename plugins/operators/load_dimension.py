from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    Dimension table loading, takes table name and sql for creation.
    Because dimension tables are small, they are always deleted before
    loading. 
    """

    ui_color = '#80BD9E'

    drop_template = SqlQueries.table_drop

    @apply_defaults
    def __init__(self,
                conn_id = "aws_credentials",
                sql_load_statement = "", 
                sql_create_statement = "",
                table_name = "",
                truncate = True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id 
        self.sql_create = sql_create_statement
        self.sql_load = f"INSERT INTO {table_name} {sql_load_statement}"
        self.table_name = table_name
        self.truncate = truncate 

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