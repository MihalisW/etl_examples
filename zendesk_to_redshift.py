#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

"""
+-----------------------------------------------------------------------------+
|  This ETL process queries the Zendesk API which responds with a JSON file   |
|  containing key values which describe various properties of created tickets,|
|  such as datetime, advisor, order number, sku, etc.       				  |
| 																		      |
|  This file is then transformed in preparation for loading into Redshift.    |
| 																		      |
|  The reason that there are two pararel processes running is because there   | 
|  are two ways for creating forms on zendesk: either through a preset form,  | 
|  or through a custom one. This distinction is important to the business for |
|  various reasons so all tickets are from the onset bucketed as either form, |
|  or non-form.															      |
+-----------------------------------------------------------------------------+

"""
# Import helper classes
from zenpy import Zenpy
from zenpy import ZenpyException
import boto3
import datetime
import json
import re
import psycopg2
import time
from tqdm import tqdm

################################Required connections,clients, and data.#######################################

# Connect to target database (two instances - one for form tickets and one for non-form tickets).
redshift_form = "host='some-resredshiftcluster-some.cluster.eu-west-1.redshift.amazonaws.com' dbname='public' user='user' password='XXX' port='5439'"
rs_pgconn_form = psycopg2.connect(redshift_form)
redshift_nonform = "host='some-resredshiftcluster-some.cluster.eu-west-1.redshift.amazonaws.com' dbname='public' user='user' password='XXX' port='5439'"
rs_pgconn_nonform = psycopg2.connect(redshift_nonform)

#set autocommit to true
rs_pgconn_form.set_session(autocommit=True)
rs_pgconn_nonform.set_session(autocommit=True)

# Create a cursor for target.
rs_cursor_form = rs_pgconn_form.cursor()
rs_cursor_nonform = rs_pgconn_nonform.cursor()

#Declare query in order retrieve most recent record in target db
sql_form = """
SELECT MAX(created_at) FROM zendesk.tickets WHERE LENGTH(ticket_form)>0
"""
sql_nonform = """
SELECT MAX(created_at) FROM zendesk.tickets WHERE LENGTH(ticket_form)=0
"""

#Execute queries 
rs_cursor_form.execute(sql_form)
rs_cursor_nonform.execute(sql_nonform)

s3 = boto3.client('s3'   
                  ,aws_access_key_id='aws_access_key_id'
                  ,aws_secret_access_key='aws_access_key_id'
                  ,region_name='region-sub_region-number')

# Declare Zendesk credentials object
creds = {
    'email' : '<email>',
    'token' : '<secret_token>',
    'subdomain': '<company_domain>'
    }

# Instantiate Zendesck client
zenpy_client = Zenpy(**creds)

#define date range variables
query_result_form = rs_cursor_form.fetchall()
for i in query_result_form:
    start_day_form = datetime.datetime.strptime(str(i[0]), '%Y-%m-%d %H:%M:%S')  
end_day_form = datetime.datetime.now() 

query_result_nonform = rs_cursor_nonform.fetchall()
for i in query_result_nonform:
    start_day_nonform = datetime.datetime.strptime(str(i[0]), '%Y-%m-%d %H:%M:%S')  
end_day_nonform = datetime.datetime.now() 

ticket_forms = ['Pre sale product enquiry'
                ,'Pre sale delivery enquiry'
                ,'Pre sale invoice enquiry'
                ,'Pre sale service enquiry'
                ,'Pre despatch order enquiry'
                ,'Post despatch delivery enquiry'
                ,'Post despatch returns enquiry'
                ,'Post despatch item damaged'
                ,'Technical enquiry'
                ,'Post despatch general enquiry']


##########################Functions#################################

#Function to extract order numbers from ticket subject line. 
def extract_order_number(string):
    try:
        found = re.search('(1000[0-9]{5})', string).group(1)
    except AttributeError:
        # order number not found
        found = '0' 
    return found

#Function to get timestamp in appropriate format.
def extract_timestamp(string):
    try:
        found1 = re.search('([0-9-]{10})T([0-9:]{8})Z', string).group(1)
        found2 = re.search('([0-9-]{10})T([0-9:]{8})Z', string).group(2)
        found0 = found1 + ' ' + found2
        post_replace = found0.replace('", "',' ').replace('"','')
    except AttributeError:
        # no timestamp or invalid format
        post_replace = None 
    return post_replace

#Function to extract skus from ticket description and return as a comma seperated list.
def extract_skus(text):
    i = 1
    result = []
    for line_number,line in enumerate(text.split('\n')):
        for word_number, word in enumerate(line.split(' ')):
            if re.match('([A-Z0-9]{8}-[n 0-9]{3})', word): 
                result.append(str(re.match('([A-Z0-9]{8}-[0-9]{3})', word).group(1))) 
                i = i + 1
    return result

#function to copy api results from s3 to Redshift utilising the native copy command
def zendesk_bucket_to_warehouse(file):
    copycommand = """ 
    COPY zendesk.tickets 
    FROM 's3://json-repository/""" + file + """'
    CREDENTIALS 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
    JSON 's3://json-repository/zendesk_data_json_paths.json'
    ACCEPTINVCHARS;"""

    try:
        rs_cursor_form.execute(copycommand)
    except Exception as e:
        print (e)
    else:    
        pass
		
    #Utility function to delete duplicate rows by the end of the process.
def delete_duplicates():
    deletecommand = """ 
        DELETE zendesk.tickets 
         WHERE LENGTH(ticket_form)=0
           AND id IN (SELECT id 
                        FROM (SELECT id
                                    ,COUNT(*) AS count_ 
                                FROM zendesk.tickets 
                            GROUP BY 1
                             ) 
                       WHERE count_ = 2
                     );"""

    try:
        rs_cursor_nonform.execute(deletecommand)
        rows_deleted = rs_cursor_nonform.rowcount
        return rows_deleted
    except Exception as e:
        print (e)

def ticket_extractor(start,end,ticket_form,form_type):
    if form_type == 'Form':
        for forms in tqdm(ticket_form, leave=True):   
            #query tickets from Zendesk api and write results to file
            file_name = time.strftime("%d_%m_%Y") +'_zendesk_tickets'+ str(forms) + '.json'
            with open(file_name,'w') as out_file:
                try:
                    for ticket in zenpy_client.search(created_between=[start, end],type='ticket',form=forms):
                        ticket_schema = {
                                        'id' : str(ticket.id)
                                        ,'ticket_form' : str(forms)
                                        ,'subject' : str(ticket.subject)
                                        ,'skus' : extract_skus(ticket.description)
                                        ,'order_id' : extract_order_number(str(ticket.subject))
                                        ,'status' :  str(ticket.status)
                                        ,'type' :  str(ticket.type)
                                        ,'description' : str(ticket.description)
                                        ,'created_at' : extract_timestamp(str(ticket.created_at))
                                        ,'brand_id' : str(ticket.brand_id)
                                        ,'group_id' : str(ticket.group_id)
                                        ,'assignee_id' : str(ticket.assignee_id)
                                        ,'assignee' : str(ticket.assignee)
                                        ,'submitter' : str(ticket.submitter)
                                        ,'recipient' : str(ticket.recipient)
                                        ,'raw_subject' : str(ticket.raw_subject)
                                        ,'tags' :  str(ticket.tags)
                                        ,'is_public' : str(ticket.is_public)
                                        ,'satisfaction_rating' :  str(ticket._satisfaction_rating)
                                        ,'allow_channelback' : str(ticket.allow_channelback)
                                        ,'etl_tstamp' : str(datetime.datetime.now())
                                        }
                        ticket_json = (json.dumps(ticket_schema))
                        out_file.write(str(ticket_json))
                except ZenpyException as error:
                    print('Problem connecting to Zendesk API while getting form tickets. {}'.format(error))
                    sys.exit(1)
    else:
        #query tickets from Zendesk api and write results to file
        file_name = time.strftime("%d_%m_%Y") +'_zendesk_tickets.json'
        with open(file_name,'w') as out_file:
            try:
                for ticket in tqdm(zenpy_client.search(created_between=[start, end],type='ticket'), leave=True):
                    ticket_schema = {
                                    'id' : str(ticket.id)
                                    ,'ticket_form' : ""
                                    ,'subject' : str(ticket.subject)
                                    ,'skus' : extract_skus(ticket.description)
                                    ,'order_id' : extract_order_number(str(ticket.subject))
                                    ,'status' :  str(ticket.status)
                                    ,'type' :  str(ticket.type)
                                    ,'description' : str(ticket.description)
                                    ,'created_at' : extract_timestamp(str(ticket.created_at))
                                    ,'brand_id' : str(ticket.brand_id)
                                    ,'group_id' : str(ticket.group_id)
                                    ,'assignee_id' : str(ticket.assignee_id)
                                    ,'assignee' : str(ticket.assignee)
                                    ,'submitter' : str(ticket.submitter)
                                    ,'recipient' : str(ticket.recipient)
                                    ,'raw_subject' : str(ticket.raw_subject)
                                    ,'tags' :  str(ticket.tags)
                                    ,'is_public' : str(ticket.is_public)
                                    ,'satisfaction_rating' :  str(ticket._satisfaction_rating)
                                    ,'allow_channelback' : str(ticket.allow_channelback)
                                    ,'etl_tstamp' : str(datetime.datetime.now())
                                    }
                    ticket_json = (json.dumps(ticket_schema))
                    out_file.write(str(ticket_json))
            except ZenpyException as error:
                print('Problem connecting to Zendesk API while getting non-form tickets. {}'.format(error))
                sys.exit(1)
                
def stage_n_load(ticket_form,form_type):
    if form_type == 'Form':
        
        for form in tqdm(ticket_form, leave=True):

            stage_file = time.strftime("%d_%m_%Y") +'_zendesk_tickets'+ str(form) + '.json'

            #stick json into s3
            s3.upload_file(stage_file,'json-repository',stage_file)

            #execute copy function
            zendesk_bucket_to_warehouse(stage_file)
    else:
        stage_file = time.strftime("%d_%m_%Y") +'_zendesk_tickets.json'
    
        #stick json into s3
        s3.upload_file(stage_file,'json-repository',stage_file)

        #execute copy function
        zendesk_bucket_to_warehouse(stage_file)
            
    
def etl():
    """ETL begins"""
    
    #Extract and transform.
    print('Form tickets to be extracted.')
    ticket_extractor(start_day_form,end_day_form,ticket_forms,'Form')
    print('Form tickets have been extracted.')
    print('Non-form tickets to be extracted.')
    ticket_extractor(start_day_nonform,end_day_nonform,ticket_forms,'Nonform')
    print('Non-form tickets have been extracted.')
    
    #Stage and load.
    stage_n_load(ticket_forms,'Form')
    print('Form tickets have been loaded. Please wait for non-form tickets to load.')
    stage_n_load(ticket_forms,'Nonform')
    print('Non-form tickets have been loaded. Please wait for duplicates to be removed (if any).')
    deleted_rows = delete_duplicates()
    print('{0} Duplicate(s) have been removed.'.format(deleted_rows))

#Make this file executable.
if __name__ == "__main__":
    etl()

#The data warehouse connection is then ordered to close.
rs_pgconn_form.close()
rs_pgconn_nonform.close()