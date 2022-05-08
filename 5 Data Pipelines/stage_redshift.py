from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    This object is loading the staging tables to Redshift from the source json files stored in AWS S3.
    """
    ui_color = '#358140'
    
    # query for copying s3 json files to Redshift
    copy_sql = """
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        json
        '{}'
        region '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 region="us-west-2",
                 sql_create_staging="",
                 *args, **kwargs):
        """
        Initialize the object with the following parameters:
            "redshift_conn_id" -> Redshift connection id
            "aws_credentials_id" -> AWS credentials
            "table" -> staging table name
            "s3_bucket" -> s3 bucket name
            "s3_key" -> file path in s3 bucket
            "json_path" -> json file path, default "auto"
            "region" -> Redshift region, default "us-west-2"
            "sql_create_staging" -> sql query for creating staging table
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.region = region
        self.sql_create_staging = sql_create_staging

    def execute(self, context):
        """
        Operator execute function: 
            1. Get AWS credentials and connect to Redshift.
            2. Creat staging table and copy the json files to the staging table created.
        """
        # AWS credentails
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # drop table
        self.log.info("Drop Redshift table")
        redshift.run("DROP TABLE IF EXISTS {};".format(self.table))
        self.log.info("Drop Redshift table successfully")
        
        # create table
        self.log.info("Creating staging table")
        redshift.run(self.sql_create_staging)     
        self.log.info("Creating staging table successfully")
        
        # copy the json files from s3 to Redshift staging table created
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,
            self.region
        )
        redshift.run(formatted_sql)
        self.log.info("Copying data from S3 to Redshift successfully")




