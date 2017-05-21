#!/usr/bin/python

"""
+-----------------------------------------------------------------------+
|  This is an ETL process which extracts invoice line data from the Xero|      
|  finance application and stages it into S3 before loading into the    |
|  Redshift data Warehouse. 						|
|   									|
|  The overall pipeline can be summarised as: 				|
|   									|
|  Xero API (Extract, Transform) ===> S3 (Stage) ===> Redshift (Load)	|
|									|									|
|  The below script takes care of the first part.			|
+-----------------------------------------------------------------------+

"""

import psycopg2
import sys
import pymysql
import csv
import time
import json
import datetime
import boto3
import os
from xero import Xero
from xero.auth import PrivateCredentials

def get_line_items(inv,out_file,counter):

    date_handler = lambda obj: (
    obj.isoformat()
    if isinstance(obj, datetime.datetime)
    or isinstance(obj, datetime.date)
    else None
    )

    l = 0
    y = json.dumps(xero.invoices.get(inv),default=date_handler)
    x = json.loads(y)
    line_item = []
    while l <= (len(x[0]['LineItems'])-1):
        line_item.append((x[0]['LineItems'][l]))
        line_item[l]['InvoiceID'] = x[0]['InvoiceID']
        line_item[l]['ContactID'] = x[0]['Contact']['ContactID']
        l = l + 1
    line_item1 = json.dumps(line_item)
    line_item2 = line_item1[1:-1]
    line_item3 = line_item2.replace('}, {','} {')
    line_item4 = str(line_item3)
    counter.append(line_item4)
    out_file.write(str(line_item4))

#create client for S3
s3 = boto3.client('s3'
                   ,aws_access_key_id='XXXXXXXXXXXXXXX'
                   ,aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXX'
                   ,region_name='region-subregion-number')

#retrieve RSA key
with open('/tmp/private_key_xero.key') as keyfile:
    rsa_key = keyfile.read()

#authorise API access
credentials = PrivateCredentials('key', rsa_key)
xero = Xero(credentials)

# Connect to target database (Redshift).
redshift = "host='some-resredshiftcluster-some.cluster.eu-west-1.redshift.amazonaws.com' dbname='public' user='user' password='XXX' port='5439'"
rs_pgconn = psycopg2.connect(redshift)

#set autocommit to true
rs_pgconn.set_session(autocommit=True)

# Create a cursor for target.
rs_cursor = rs_pgconn.cursor()

#query to extract invoices that need invoice lines in redshift - the query execution follows straight after
sql = """
         SELECT DISTINCT i.invoice_id
           FROM xero.invoices i
          WHERE i.date >= (SELECT MAX(date)
                             FROM xero.invoices
                            WHERE NOT EXISTS (SELECT DISTINCT invoice_id
                                                                 FROM xero.invoice_line_items2))
"""

rs_cursor.execute(sql)

invoice_ids = rs_cursor.fetchall()

#this will be the file name for our S3 json
file_name = time.strftime("%d_%m_%Y") + '_xero_line_items.json'

#begin querying Xero API
with open(file_name,'w', encoding='utf-8') as xero_invoices:
    line_item = []
    line_items_counter = []
    seconds = 0
    total_run_time = 0
    for invoice in invoice_ids:
        #iteration start time
        t0 = time.clock()

        #retrieve line items
        y = get_line_items(*invoice,xero_invoices,line_items_counter)

        #line_items_counter.append(line_items)
        #line_items_counter.append(y)

        #calculate run time and append to total time
        seconds += time.clock() - t0
        total_run_time += seconds
		
		#Check for the following limitation: No more than 59 calls per 59 seconds.
        if len(line_items_counter) == 59 or seconds >= 59:
            time.sleep(60)
            line_items_counter = []
            seconds = 0
        else:
            continue

#stick into S3 bucket
s3.upload_file(file_name,'json-repository',file_name)

#remove staging file from server memory
os.remove(file_name)

#close connection to redshift
rs_cursor.close()
