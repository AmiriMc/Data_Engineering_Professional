import time
import os
import boto3
import json

################## Import plugins/modules from sqlqueries folder ##########################
import glob

print("etl.py: Importing modules from sqlqueries folder")
# exclude = ['__pycache__', '__init__.py', '*.txt', 'New Folder']

# Do recursive read on top directory, create list of all directories and files.
print("etl.py: Creating list of directories and files")

root_dir = 'sqlqueries' # Name of top level package (folder).
all_paths = []
for path in glob.iglob(root_dir + '**/**', recursive=True):
    all_paths.append(path)
print('etl.py: all_paths:', all_paths)

# Using filenames list, create corresponding Boolean list for directories that contain files.
isFile = []
for item in all_paths:
    isFile.append(os.path.isfile(item))
print('etl.py: isFile:', isFile)

# Use all_paths and isFile lists to pop filenames items and create new list that contains files only.
file_paths = []
for idx, item in enumerate(isFile):
    if item == True:
        popped_item = all_paths.pop(idx)
        file_paths.append(popped_item)
        all_paths.insert(idx, popped_item) # push item back into list for proper indexing
    #else:
        #print("False found at index:", idx)
print('etl.py: file_paths:', file_paths)

# Create list of paths that have .py file extensions only.
py_paths = []
for item in file_paths:
    if item.endswith(".py"):
        py_paths.append(item)
print('etl.py: py_paths:', py_paths)

# Remove all __init__.py files.
result = []
for index, element in enumerate(py_paths, start=0):
    if os.path.basename(element) != '__init__.py':
        popped_item = py_paths.pop(index)
        py_paths.insert(index, popped_item)
        result.append(popped_item)

# Remove .py extension and replace path slashes (\\) with a period (.)
# so that it can be dynamically called with importlib.
modules = []
for item in result:
    modules.append(item.replace('.py', '').replace('\\', '.').replace('/', '.'))
#print('etl.py: modules:', modules)

import importlib
plugin_module_list = []
def init_modules():
    for module in modules:
        print('module:', module)
        plugin_module_list.append(importlib.import_module(module))
    
    return plugin_module_list

plugin_module_list = init_modules()
###########################################################################################

# I need to use a class here, a 'sql_queries' class and then a method for each sql_queries_<ddb table>.py

# Create client for Secrets Manager
client = boto3.client('secretsmanager')

def create_database(mycursor, mydb, ddbTable, db_name, plugin):
    ''' 
    Create the db if needed
    '''
    print('Running etl.py function: create_database')
    
    # DROP db if needed. Do not want to run this in the prod version
    mycursor.execute(f"DROP DATABASE IF EXISTS {plugin.db_name}")

    # CREATE db
    create_db = plugin.create_db
    mycursor.execute(create_db)

    # USE db
    mycursor.execute(f"USE {plugin.db_name}")
    mydb.commit()
    print('finished creating database')

def create_tables(mycursor, mydb, ddbTable, db_name, plugin):
    ''' 
    Create tables if needed
    '''
    print('Running etl.py function: create_tables')
    mycursor.execute(f"USE {db_name}") # This in case create_database() is not called
    for query in plugin.create_table_queries:
        mycursor.execute(query)
        #mydb.commit()
    print('finished creating tables')

def load_tables_from_s3(mycursor, mydb, s3_path, ddbTable, db_name, plugin):
    
    ''' 
    Load MySQL table(s) from the data file(s) in s3
    '''
    print('Running etl.py function: load_tables_from_s3')
    print("s3_path:", s3_path)
    
    load_s3_data = plugin.load_from_s3(s3_path)
    mycursor.execute(load_s3_data)
    #mydb.commit()
    print('load_s3_data:', load_s3_data)
    print('finished loading tables')

    #mycursor.execute("SELECT * FROM xxxx_sms_log_json LIMIT 10;")
    for query in plugin.select_table_queries:
        mycursor.execute(query)

    myresult = mycursor.fetchall()
    for x in myresult:
        print('results of fetchall():', x)
    print('Finished loading tables from s3')

def insert_tables(mycursor, mydb, ddbTable, db_name, plugin):
    """
    ETL and insert data into respective tables
    """
    print('Running etl.py function: `insert_tables`')
    for query in plugin.insert_table_queries:
        mycursor.execute(query)
        #mydb.commit()
    
    table_list = []
    mycursor.execute("SHOW TABLES;")
    myresult = mycursor.fetchall()
    for x in myresult:
        x = ''.join(x) # convert tuple to string
        table_list.append(x)
    print('table_list:', table_list)

    for table in table_list:
        mycursor.execute(f"SELECT * FROM {table} LIMIT 10;")
        myresult = mycursor.fetchall()
        for x in myresult:
            print(f'{table}:', x)
    
    print('Finished ETL and inserting data into tables')

    
def main(s3_path):
    ''' 
    Connect to the database, call functions to create/load tables
    '''
    try:
        # Connect to db
        print('Running etl.py function: main, connecting to database...')
        import mysql
        import mysql.connector

        # Database credentials
        
        # Access Secrets Manager
        #secret_name = "customers/Aurora/MySQL"
        #region_name = "us-west-1"
        response = client.get_secret_value(
            SecretId='customers/Aurora/MySQL'
        )
        secretDict = json.loads(response['SecretString'])
        #print('secretDict:', secretDict)

        mydb = mysql.connector.connect(
        host=secretDict['host'],
        user=secretDict['username'],
        password=secretDict['password']
        )
        mycursor = mydb.cursor()
        print('mydb:', mydb)

        # Get DDB table name. This will be the name for any script called from the sqlqueries package.
        ddbTable = s3_path.split('/')[3] # Get DDB table name from S3 object name. It is the first directory after root.
        ddbTable = ddbTable.replace('-', '_')
        print('ddbTable:', ddbTable)

        #db_name = eval(f"{ddbTable}.db_name")
        # Use .split() to get the names of the modules (python files)
        modules_names = []
        for item in modules:
            modules_names.append(item.split('.')[2])
        print('modules_names:', modules_names)

        # Is there an available sql_queries modeule/plugin? Find match.
        plugin_module_index = modules_names.index(ddbTable)
        print('plugin_module_index:', plugin_module_index)
        plugin_module = plugin_module_list[plugin_module_index]
        print('plugin_module:', plugin_module)
        plugin = plugin_module.Plugin("IS this thing working??!", key=123)
        print('plugin:', plugin)
        db_name = plugin.db_name
       
        print('db_name:', db_name)


        # Call functions
        create_database(mycursor=mycursor, mydb=mydb, ddbTable=ddbTable, db_name=db_name, plugin=plugin)
        create_tables(mycursor=mycursor, mydb=mydb, ddbTable=ddbTable, db_name=db_name, plugin=plugin )
        load_tables_from_s3(mycursor=mycursor, mydb=mydb, s3_path=s3_path, ddbTable=ddbTable, db_name=db_name, plugin=plugin)
        insert_tables(mycursor=mycursor, mydb=mydb, ddbTable=ddbTable, db_name=db_name, plugin=plugin)

        # Commit all queries
        mydb.commit()

        # Close connection to db
        mycursor.close()

    except Exception as e:
      print('exception in etl.py:', e)
      return("Error in etl.py")


if __name__ == "__main__":
    main()