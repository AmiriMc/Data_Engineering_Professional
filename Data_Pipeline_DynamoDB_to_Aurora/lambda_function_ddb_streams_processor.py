import json
import boto3
import ast

#FirehoseClient = session.client('firehose')
client = boto3.client('firehose')
dynamo_client = boto3.resource('dynamodb', region_name='us-west-1')

def lambda_handler(event, context):
    
    #if not dynamodb:
    #    #dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    #    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    
    print('-------------------------')
    try:
        #1. Iterate over each record
        for record in event['Records']:
            #2. Handle event type
            if record['eventName'] == 'INSERT':
                handle_insert(record)
            elif record['eventName'] == 'MODIFY':
                handle_modify(record)  
            elif record['eventName'] == 'REMOVE':
                handle_remove(record)  
        print('-------------------------')    
    except Exception as e:
        print(e)
        print('-------------------------')
        return "Error in DynamoDB-Streams-Processor Lambda function."
    
    return event

  
###############################################      
def handle_insert(record):
    print('Handling INSERT event')
    
    #3a. Get newImage content`
    newImage = record['dynamodb']['NewImage']
    
    print('newImage data type', type(newImage))
    eventID = record['eventID']
    eventName = record['eventName']
    
    print('eventID:', eventID)
    print('newImage:', newImage)
    #print('record:', record) # Shows the entire record

    newImage = str(newImage) # convert to string
    newImage = ast.literal_eval(newImage) # convert back to json
    # newImage = json.loads(newImage) # convert back to json (.loads throws an error "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)")
    
    # Get DDB table name
    ddbARN = record['eventSourceARN']
    ddbTable = ddbARN.split(':')[5].split('/')[1]
    print("DynamoDB table name: " + ddbTable)
  
    # Send to Kinesis Firehose. Kinesis Firehose stream should have the exact same name as the DDB table that triggered Lambda.
    response = client.put_record(
        DeliveryStreamName=ddbTable,
        Record={
            'Data': json.dumps(newImage, indent=None, ensure_ascii=True)
        }
    )
     
    #3c. Print it out
    print('New row added')
    print('Done hanlding INSERT event')
    
    #return 'Successfully processed {} records.'.format(len(event['Records']))
