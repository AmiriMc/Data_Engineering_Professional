import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "quotewizard", table_name = "quotewizard_inbound_sms_log", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "quotewizard", table_name = "quotewizard_inbound_sms_log", transformation_ctx = "datasource0")

## Add the input file name column
##datasource1 = datasource0.toDF().withColumn("input_file_name", input_file_name())
## Convert DataFrame back to DynamicFrame
##datasource2 = datasource0.fromDF(datasource1, glueContext, "datasource2")

## @type: ApplyMapping
## @args: [mapping = [("account_id", "string", "account_id", "string"), ("item_id", "string", "item_id", "string"), ("phone", "string", "phone", "string"), ("log_time", "string", "log_time", "timestamp")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("account_id", "string", "account_id", "string"), ("item_id", "string", "item_id", "string"), ("phone", "string", "phone", "string"), ("log_time", "string", "log_time", "timestamp")], transformation_ctx = "applymapping1")

applymapping2 = applymapping1.toDF().withColumn("input_file_name", input_file_name())
applymapping3 = applymapping1.fromDF(applymapping2, glueContext, "applymapping3")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://aws-glue-scripts-011430920755-us-west-1"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasource_df = applymapping3.repartition(1)
datasink2 = glueContext.write_dynamic_frame.from_options(frame = datasource_df, connection_type = "s3", connection_options = {"path": "s3://aws-glue-scripts-011430920755-us-west-1/glue_etl_output/inbound/"}, format = "csv", transformation_ctx = "datasink2")
job.commit()