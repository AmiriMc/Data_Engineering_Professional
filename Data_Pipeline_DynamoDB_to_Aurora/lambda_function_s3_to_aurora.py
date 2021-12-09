import json
import boto3
import urllib
import os
import etl

# Future: Will need to create/access a connection pool.

s3 = boto3.client('s3') 

def lambda_handler(event, context):
   print('S3-to-Aurora-MySQL Lambda function called')
   
   #1 Get the bucket name
   bucket = event['Records'][0]['s3']['bucket']['name']
   print('bucket:', bucket)

   #2 Get the file/key name
   key =urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
   print('key:', key)

   try:

      #3 Fetch the file from s3
      response = s3.get_object(Bucket=bucket, Key=key)
      #print('response', response)

      #4 Deserialize the file's content
      text = response["Body"].read().decode()
      print('text:', text)
      print('text datatype', type(text))

      #5 Run etl.py script (which calls sql_queries.py) to etl and load the data into Aurora MySQL
      s3_path = f's3://{bucket}' + "/" + key
      ddbTable = s3_path.split('/')[3] # Get DDB table name from S3 object name. It is the first directory after root.
      etl_filename = 'etl_' + ddbTable + '.py' # Could make this conditional so that if fail, load regular etl.py
      etl.main(s3_path) # run etl.py script

      #6 Copy the original s3 object into the "Processed" path as backup of the raw data
      s3_resource = boto3.resource('s3')
      bucket_backup = "customer-backup" # Bucket for raw data backup
      key_backup = "Processed/" + key # Path for raw data backup
      copy_source = {
      'Bucket': bucket,
      'Key': key
      }
      bucket_backup = s3_resource.Bucket(bucket_backup)
      bucket_backup.copy(copy_source, key_backup) # Copy original object to backup directory

      print('s3 bucket:', bucket)
      print('s3 bucket_backup', bucket_backup)

      #7 Delete the original s3 object
      #s3_resource.Object(bucket, key).delete()


   except Exception as e:
      print('exception in lambda_function.py:', e)
      return("Error in S3-to-Aurora-MySQL Lambda function")
        
   return event
