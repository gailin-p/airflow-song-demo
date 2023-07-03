from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator that checks for null values in one or more tables. 
    """

    check_table_query = "SELECT COUNT(*) FROM {table} WHERE {column} is NULL"


    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                conn_id = "aws_credentials",
                table_list = [], 
                column_list = [],
                max_allowed = 0,
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_list = table_list
        self.column_list = column_list
        self.max_allowed = max_allowed

        # Check that user configured correctly 
        assert len(column_list) == len(table_list)

    def execute(self, context):
        redshift = PostgresHook(self.conn_id)
        for i in range(len(self.table_list)): 
            q = self.check_table_query.format(
                table=self.table_list[i],
                column=self.column_list[i]
            )
            assert redshift.get_records(q)[0] <= self.max_allowed