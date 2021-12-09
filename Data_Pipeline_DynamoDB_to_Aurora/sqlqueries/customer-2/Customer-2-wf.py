# DB NAME
db_name = "customer_2_db"

# CREATE DB, IF NEEDED

create_db = f"CREATE DATABASE IF NOT EXISTS {db_name}"

# DROP TABLES

movies_json_drop = "DROP TABLE IF EXISTS customer_2_sms_log_json"


# CREATE TABLES

customer_2_sms_log_json_create = ("""
    CREATE TABLE IF NOT EXISTS customer_2_sms_log_json (
        id int auto_increment PRIMARY KEY, 
        json_column JSON
    );
""")

customer_2_sms_log_json_all_items_create = (""" 
    CREATE TABLE IF NOT EXISTS customer_2_sms_log_all_items (
        item_id VARCHAR(255) PRIMARY KEY,
        phone INT,
        message VARCHAR(255),
        quoteID VARCHAR(255),
        log_time datetime,
        phone_10 VARCHAR(20),
        caller_id VARCHAR(20),
        account_id VARCHAR(50),
        contractID VARCHAR(50),
        phone_type VARCHAR(20),
        carrier_sid VARCHAR(255),
        term_carrier VARCHAR(50),
        carrier_price VARCHAR(50),
        carrier_status VARCHAR(50),
        medium_lead_id VARCHAR(255),
        message_item_id VARCHAR(255),
        sms_campaign_id VARCHAR(255),
        carrier_error_msg VARCHAR(255),
        carrier_error_code VARCHAR(255),
        carrier_num_segments int,
        carrier_subresources VARCHAR(255),
        feedback VARCHAR(255),
        media VARCHAR(255)
    );
""")

# LOAD S3 DATA
# The load data statement was moved to etl.py
# Need to move it back into this script. Create a function and call function from etl.py

def customer_2_sms_log_load(s3_path):
    ''' 
    Load MySQL table(s) from the data file(s) in s3
    '''
    print('Running sql_queries.py function: load_tables_from_s3')
    print("s3_path:", s3_path)
    
    # Create load statement
    load_s3_data = (f"""
    LOAD DATA FROM S3 '{s3_path}'
	    INTO TABLE customer_2_sms_log_json (json_column);
    """)

    return load_s3_data

# INSERT DATA

customer_2_sms_log_insert = ("""
    INSERT INTO customer_2_sms_log_info(id, year, title, info) 
        SELECT id, json_column->>'$.year' year, json_column->>'$.title' title, json_column->>'$.info' info
            FROM movies_json;
""")

customer_2_sms_log_json_all_items_insert = ("""
    INSERT INTO customer_2_sms_log_all_items (item_id, phone, message, quoteID, log_time, phone_10, caller_id, account_id, contractID, phone_type, 
	    carrier_sid, term_carrier, carrier_price, carrier_status, medium_lead_id, message_item_id, sms_campaign_id, carrier_error_msg, 
		    carrier_error_code, carrier_num_segments, carrier_subresources, feedback, media) 
    SELECT 	json_column->> '$.item_id' item_id, json_column->> '$.phone' phone, json_column->> '$.message' message, json_column->> '$.quoteID' quoteID, 
		json_column->> '$.log_time' log_time, json_column->> '$.phone_10' phone_10, json_column->> '$.caller_id' caller_id,
        json_column->> '$.account_id' account_id, json_column->> '$.contractID' contractID, json_column->> '$.phone_type' phone_type, 
        json_column->> '$.carrier_sid' carrier_sid, json_column->> '$.term_carrier' term_carrier, json_column->> '$."carrier:price"' carrier_price,
        json_column->> '$."carrier:status"' carrier_status, json_column->> '$.medium_lead_id' medium_lead_id, json_column->> '$.message_item_id' message_item_id,
        json_column->> '$.sms_campaign_id' sms_campaign_id, json_column->> '$."carrier:error_msg"' carrier_error_msg, 
        json_column->> '$."carrier:error_code"' carrier_error_code, json_column->> '$."carrier:num_segments"' carrier_num_segments, 
        json_column->> '$."carrier:subresources"' carrier_subresources, json_column->> '$."carrier:subresources".feedback' feedback, 
        json_column->> '$."carrier:subresources".media' media
    FROM customer_2_sms_log_json;
""")

# QUERY LISTS
create_table_queries = [customer_2_sms_log_json_create, customer_2_sms_log_json_all_items_create]
insert_table_queries = [customer_2_sms_log_json_all_items_insert]
drop_table_queries = []
