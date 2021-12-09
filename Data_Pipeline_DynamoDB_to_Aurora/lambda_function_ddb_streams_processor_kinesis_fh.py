import json
import boto3
import base64
import ast
from boto3.dynamodb.types import TypeDeserializer
import decimal
from decimal import *

output = []

def lambda_handler(event, context):
    
    try:
        data_w_new_line = add_new_line(event)
    except Exception as e:
        print(e)
    
    return data_w_new_line # you do not do `return event` here; results in error
    
def add_new_line(event):

    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        print('payload:', payload)
        print('payload type:', type(payload))
        
        # Lazy-eval the dynamodb attribute (boto3 is dynamic!)
        boto3.resource('dynamodb')

        # To go from low-level format to python (remove data type descriptors)
        payload = json.loads(payload) # convert to string
        deserializer = boto3.dynamodb.types.TypeDeserializer()
        python_data = {k: deserializer.deserialize(v) for k,v in payload.items()}
        print('python_data 1:', python_data)
        
        python_data = replace_decimals(python_data)
        print('python_data 2:', python_data)
        print('python_data 2 type:', type(python_data))
        python_data = json.dumps(python_data)
        
        print('python_data 3 type:', type(python_data))
        #python_data = ast.literal_eval(python_data) # convert back to json
        #python_data = json.loads(python_data) # convert back to json
        print('python_data 3:', python_data)


        #assert low_level_data == low_level_copy
        
        row_w_newline = python_data + "\n"
        print('row_w_newline:', row_w_newline)
        row_w_newline = base64.b64encode(row_w_newline.encode('utf-8'))
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': row_w_newline
        }
        output.append(output_record)

    print('Added new line to {} records.'.format(len(event['records'])))
    
    return {'records': output}

def replace_decimals(obj):
    """
    Convert all whole number decimals in `obj` to integers
    """
    if isinstance(obj, list):
        return [replace_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: replace_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, decimal.Decimal):
        return int(obj) if obj % 1 == 0 else obj
    return obj




        