from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    """
    Create staging tables and load data using COPY. 
    Uses s3 paths configured in Airflow variables. 
    """

    ui_color = '#358140'
    drop_template = SqlQueries.table_drop

    copy_template = """
        copy staging_events from {}
        iam_role {}
        json {} region 'us-west-2';
    """ 

    @apply_defaults
    def __init__(self,
            redshift_conn_id="aws_credentials",
            table_name="staging",
            s3_path="",
            create_table_sql="",
            template_path="auto",
            iam_role="",
            year="2018",
            month="11",
            *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = redshift_conn_id
        self.table_name = table_name 
        self.s3_path = s3_path
        self.create_table_sql = create_table_sql
        self.template_path= template_path
        self.iam_role = iam_role
        # Could use these to get a subset of log data (not implemented)
        self.year = year 
        self.month = month

    def execute(self, context):
        print(self.conn_id)
        redshift = PostgresHook(self.conn_id)
        del_table = self.drop_template.format(self.table_name)
        redshift.run(del_table)
        redshift.run(self.create_table_sql)
        copy_sql = self.copy_template.format(
            self.s3_path,
            self.iam_role,
            self.template_path,
        )
        redshift.run(copy_sql)





