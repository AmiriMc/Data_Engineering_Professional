# Scalable Enterprise Quality Data Pipeline 

## Introduction

This data pipeline architecture performs ETL and pipes data from Amazon DynamoDB (with Streams enabled), which is a NoSQL database, to Amazon Aurora (RDS) and backs up the ETL data into Amazon S3. 

There is a dynamic Plug and Play (PnP) architecture that allows the developer to quickly ETL new DynamoDB tables simply by:
1. Creating the custom ETL Python script,
2. ensuring that the filename matches the DynamoDB table name exactly,
3. and then dropping that script into the `sqlqueries` folder. The program will automatically read these new scripts without needing any code changes to the other three scripts used in the pipeline or any changes in configuration to any of the other resources.

## Features
* **Scalable and enterprise quality**. This data pipeline architecture is essentially serverless and can quickly scale to hundreds of thousands and even millions of parallel executions and is thus robust for production level usage.
* **Dynamic Plug and Play (PnP) architecture** for adding new SQL Python scripts for the ETL. All files in the PnP folder are checked recursively and every file is inspected and listed if they pass the simple verification steps.
* The AWS Lambda function scripts are **abstracted** so that they can be used as **templates for reuse**.

## Folders and Python Files
* `sqlqueries`: This folder is where the sql_queries_xx.py files should be placed. The lambda function will recurrsively read all folders in this directory and create a list of all "plugin modules" or ETL Python files. These files get called by etl.py.
* `lambda_function_ddb_streams_processor.py`: Triggered from DynamoDB Streams and it prepares the record to be sent to Kinesis Firehose
* `lambda_function_ddb_streams_processor_kinesis_fh.py`: Provides a transformation step by adding a carriage return (or new line) to the end of the record.
* `etl.py`: This script sets up the ETL pipeline. It calls and processes all sql_queries_xx.py scripts. These scripts are found in the sqlqueries folder and the filename must match exactly the DynamoDB table name as a requirement of the Plug and Play architecture code.
* `sql_queries_xx.py`: These scripts perform the actual ETL using SQL. Tables are dropped (DROP), created (CREATE TABLE), data is massaged and inserted (INSERT) into the tables, and data is selected (SELECT) for analysis.

![](https://github.com/AmiriMc/Data_Engineering_Professional/blob/main/Data_Pipeline_DynamoDB_to_Aurora/DDB_to_Aurora_Data_Pipeline_Architecture.png?raw=t)

## AWS Cloud Environment
What is not captured in the diagram is that AWS IAM Roles, Policies, and permissions had to be created and assigned to users and resources so that everything could communicate and work as expected. I also had to create a VPC with multiple public and private security groups so that our production database could be made private and not open to the public.




