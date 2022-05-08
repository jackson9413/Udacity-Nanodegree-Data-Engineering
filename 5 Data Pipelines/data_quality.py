from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This object is performing simple data quality check by getting the number of rows for all the tables created.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):
        """
        Initialize the object with the following parameters:
            "redshift_conn_id" -> Redshift connection id
            "tables" -> table list containing all the table names created
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Operator execute function: 
            1. Connect to Redshift.
            2. Get the number of rows in each table and print it. Otherwise, print error.         
        """
        # connect to redshift
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # for loop to get each table
        for table in self.tables:
            # get the number of records
            records = redshift_hook.get_records("""SELECT COUNT(*) FROM {};""".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(table))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. {} contained 0 rows".format(table))
            self.log.info("Data quality on table {} check passed with {} records".format(table, records[0][0]))
            
        