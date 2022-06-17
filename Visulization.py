import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import mysql.connector


class Tracker():
    """
    job_id, status, updated_time
    """
    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig
    def assign_job_id(self):
        job_id = str(datetime.datetime.now())
        return job_id
    def update_job_status(self, status):
        job_id = self.assign_job_id()
        print("Job ID Assigned: {}".format(job_id))
        update_time = datetime.datetime.now()
        table_name = self.dbconfig.get("postgres", "job_tracker_table_name")
        connection = self.get_db_connection()
        try:
            # [Execute the SQL statement to insert to job status table]
            SQL = 'INSERT  INTO {} (job_status,update_time) VALUES({}, {})'.format(table_name)
            VAL = (status, update_time)
            self.dbconfig.execute(SQL, VAL)
        except (Exception) as error:
            print("error executing db statement for job tracker.")
        return
    def get_job_status(self, job_id, table_name):
        self.get_db_connection()

        try:
            record = "SELECT  * FROM {} LIMIT 1".format(table_name)
            self.dbconfig.execute(record)
            return record
        except (Exception) as error:
            print("error executing db statement for job tracker.")
            return

    def get_db_connection(self):
        connection = None

        try:
            connection =  mysql.connector.connect(
                          host="localhost",
                          user="*******",
                          password="********",
                          database="LeyanaSugarStudio")
        except (Exception) as error:
            print("Error while connecting to MySQL", error)
        return connection


def run_reporter_etl(my_config):
    tracker = Tracker('analytical_etl', my_config)
    try:
        return tracker.update_job_status("success")
    except Exception as e:
        print(e)
        return tracker.update_job_status("failed")



conf = SparkConf().setAppName('Visulizatoin').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sparkContext= sc)
results = run_reporter_etl(conf)
print(results)