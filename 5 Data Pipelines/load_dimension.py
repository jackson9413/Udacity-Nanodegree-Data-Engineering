from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This object is creating dimension tables and inserting into dimension tables based on the staging tables created.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_create_normalized="",
                 sql_insert_normalized="",
                 *args, **kwargs):
        """
        Initialize the object with the following parameters:
            "redshift_conn_id" -> Redshift connection id
            "table" -> dimension table name
            "sql_create_normalized" -> sql query for creating dimension table
            "sql_insert_normalized" -> sql query for inserting into dimension table
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_create_normalized = sql_create_normalized
        self.sql_insert_normalized = sql_insert_normalized

    def execute(self, context):
        """
        Operator execute function: 
            1. Connect to Redshift.
            2. Drop dimension table if exists and create dimension table.
            3. Insert into dimension table from the staging tables created.
        """
        # connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # drop table
        self.log.info("Drop Redshift table if exists")
        redshift.run("""DROP TABLE IF EXISTS {};""".format(self.table))    
        self.log.info("Drop Redshift table successfully")
        
        # create table 
        self.log.info("Creating normalized table")
        redshift.run(self.sql_create_normalized)     
        self.log.info("Creating normalized table successfully")
        
        # insert into table
        self.log.info("Insert normalized table")
        redshift.run(self.sql_insert_normalized)     
        self.log.info("Insert normalized table successfully")
        
        
        
