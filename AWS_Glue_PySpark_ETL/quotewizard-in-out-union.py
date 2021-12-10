import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def process_outbound():
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "quotewizard", table_name = "quotewizard_sms_log", transformation_ctx = "datasource0")
    applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("phone_type", "string", "phone_type", "string"), ("item_id", "string", "item_id", "string"), ("phone", "string", "phone", "string"), ("log_time", "string", "log_time", "string")], transformation_ctx = "applymapping1")
    applymapping2 = applymapping1.toDF().withColumn("bound", lit("Outbound"))
    applymapping3 = applymapping1.fromDF(applymapping2, glueContext, "applymapping3")
    
    datasource_df_out = applymapping3.repartition(1)
    datasink2 = glueContext.write_dynamic_frame.from_options(frame = datasource_df_out, connection_type = "s3", connection_options = {"path": "s3://aws-glue-scripts-011430920755-us-west-1/glue_etl_output/outbound"}, format = "csv", transformation_ctx = "datasink2")
    datasource_df_out = datasource_df_out.toDF()
    
    return datasource_df_out
    
def process_inbound():
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "quotewizard", table_name = "quotewizard_inbound_sms_log", transformation_ctx = "datasource0")
    applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("account_id", "string", "account_id", "string"), ("item_id", "string", "item_id", "string"), ("phone", "string", "phone", "string"), ("log_time", "string", "log_time", "string")], transformation_ctx = "applymapping1")
    applymapping2 = applymapping1.toDF().withColumn("bound", lit("Inbound"))
    applymapping3 = applymapping1.fromDF(applymapping2, glueContext, "applymapping3")
    datasource_df_in = applymapping3.repartition(1)
    datasink2 = glueContext.write_dynamic_frame.from_options(frame = datasource_df_in, connection_type = "s3", connection_options = {"path": "s3://aws-glue-scripts-011430920755-us-west-1/glue_etl_output/inbound"}, format = "csv", transformation_ctx = "datasink2")
    datasource_df_in = datasource_df_in.toDF()   
    
    return datasource_df_in 

 
def union_join(df_in, df_out):
    # Union join
    df_union = df_in.union(df_out)
    df_union.write.option("header","true").mode('overwrite').repartition(1).csv("s3://aws-glue-scripts-011430920755-us-west-1/glue_etl_output/union/df_union.csv")

def main():
     df_in = process_inbound()
     df_out = process_outbound()
     union_join(df_in, df_out)
     
     
     job.commit()
     
if __name__ == "__main__":
    main()

#df_union = datasource_df_in.union(datasource_df_out)
 
'''    
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "quotewizard", table_name = "quotewizard_inbound_sms_log", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("account_id", "string", "account_id", "string"), ("item_id", "string", "item_id", "string"), ("phone", "string", "phone", "string"), ("log_time", "string", "log_time", "string")], transformation_ctx = "applymapping1")
applymapping2 = applymapping1.toDF().withColumn("bound", lit(job_name))
applymapping3 = applymapping1.fromDF(applymapping2, glueContext, "applymapping3")
datasource_df_in = applymapping3.repartition(1)
datasink2 = glueContext.write_dynamic_frame.from_options(frame = datasource_df_in, connection_type = "s3", connection_options = {"path": "s3://aws-glue-scripts-011430920755-us-west-1/glue_etl_output/inbound"}, format = "csv", transformation_ctx = "datasink2")
'''