#!/usr/bin/env python
"""
+------------------------------------------------------------------------------+
|  This ETL process queries the Zendesk API which responds with a JSON file    |
|  containing key values which describe various properties of created tickets, |
|  such as datetime, advisor, order number, sku, etc.                          |
|                                                                              |
|  This file is then transformed in preparation for loading into Redshift.     |
|                                                                              |
|  The reason that there are two processes running is because there            |
|  are two ways for creating forms on zendesk: either through a preset form,   |
|  or through a custom one. This distinction is important to the business for  |
|  various reasons so all tickets are from the onset bucketed as either form,  |
|  or non-form.                                                                |
+------------------------------------------------------------------------------+
"""
import datetime
import json
import re
import time
import os

import boto3
import psycopg2
from tqdm import tqdm
from zenpy import Zenpy, ZenpyException


def find_dates(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)

    row = cursor.fetchone()
    start_date = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.now()
    return start_date, end_date


# Function to extract order numbers from ticket subject line.
def extract_order_number(string):
    try:
        found = re.search('(1000[0-9]{5})', string).group(1)
    except AttributeError:
        # Order number not found
        found = '0'
    return found


# Function to get timestamp in appropriate format.
def extract_timestamp(string):
    try:
        match = re.search('([0-9-]{10})T([0-9:]{8})Z', string)
        found1 = match.group(1)
        found2 = match.group(2)
        found = found1 + ' ' + found2
        post_replace = found.replace('", "',' ').replace('"','')
    except AttributeError:
        # No timestamp or invalid format
        post_replace = None
    return post_replace


# Function to extract SKUs from ticket description and return as a comma seperated list.
def extract_skus(text):
    results = []
    for line in text.split('\n'):
        for word in line.split(' '):
            match = re.match('([A-Z0-9]{8}-[n 0-9]{3})', word)
            if match:
                results.append(str(match.group(1)))
    return results


# Utility function to delete duplicate rows by the end of the process.
def delete_duplicates(conn):
    command = """
      DELETE zendesk.tickets
      WHERE LENGTH(ticket_form)=0 AND id IN
        (
          SELECT id
          FROM
            (
              SELECT id, COUNT(*) AS count_
              FROM zendesk.tickets
              GROUP BY 1
            )
          WHERE count_ = 2
        );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(command)
        rows_deleted = cursor.rowcount
        return rows_deleted
    except Exception as e:
        print (e)


def make_document(ticket, form=None):
    return {
        'id': str(ticket.id),
        'ticket_form': str(form) if form else '',
        'subject': str(ticket.subject),
        'skus': extract_skus(ticket.description),
        'order_id': extract_order_number(str(ticket.subject)),
        'status': str(ticket.status),
        'type': str(ticket.type),
        'description': str(ticket.description),
        'created_at': extract_timestamp(str(ticket.created_at)),
        'brand_id': str(ticket.brand_id),
        'group_id': str(ticket.group_id),
        'assignee_id': str(ticket.assignee_id),
        'assignee': str(ticket.assignee),
        'submitter': str(ticket.submitter),
        'recipient': str(ticket.recipient),
        'raw_subject': str(ticket.raw_subject),
        'tags':  str(ticket.tags),
        'is_public': str(ticket.is_public),
        'satisfaction_rating': str(ticket._satisfaction_rating),
        'allow_channelback': str(ticket.allow_channelback),
        'etl_tstamp': str(datetime.datetime.now()),
    }


def extract_tickets(zenpy, start_date, end_date, filename, form=None):
    # Query tickets from Zendesk api and write results to file
    with open(filename, 'w') as out_file:
        try:
            for ticket in zenpy.search(
                    created_between=[start_date, end_date],
                    type='ticket',
                    form=form):
                ticket_json = json.dumps(make_document(ticket, form=form))
                out_file.write(str(ticket_json))
        except ZenpyException as error:
            sys.exit(1)


def stage_and_load_tickets(conn, s3, filename):
    s3.upload_file(filename, 'json-repository', filename)

    command = """
      COPY zendesk.tickets
      FROM 's3://json-repository/{}'
      CREDENTIALS 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
      JSON 's3://json-repository/zendesk_data_json_paths.json'
      ACCEPTINVCHARS;
    """.format(filename)
    try:
        cursor = conn.cursor()
        cursor.execute(command)
    except Exception as e:
        print (e)


def etl():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
    zenpy = Zenpy(
        email=os.getenv("ZENDESK_EMAIL"),
        token=os.getenv("ZENDESK_TOKEN"),
        subdomain=os.getenv("OUR_SUBDOMAIN"),
    )
    dsn_string = os.getenv("REDSHIFT_CONN_STRING")
    
    # Process nonform tickets
    conn = psycopg2.connect(dsn_string)
    conn.set_session(autocommit=True)
    nonform_query = "SELECT MAX(created_at) FROM zendesk.tickets WHERE LENGTH(ticket_form)=0"
    start_date, end_date = find_dates(conn, query=nonform_query)

    filename = time.strftime("%d_%m_%Y") + '_zendesk_tickets.json'
    extract_tickets(zenpy, start_date, end_date, filename)
    stage_and_load_tickets(conn, s3, filename)

    delete_duplicates(conn)
    conn.close()

    # Process form tickets
    form_dsn = "host='hostname' dbname='public' user='user' password='XXX' port='5439'"
    conn = psycopg2.connect(dsn_string)
    conn.set_session(autocommit=True)
    form_query = "SELECT MAX(created_at) FROM zendesk.tickets WHERE LENGTH(ticket_form)>0"
    start_date, end_date = find_dates(conn, query=form_query)

    ticket_forms = [
        'Pre sale product enquiry',
        'Pre sale delivery enquiry',
        'Pre sale invoice enquiry',
        'Pre sale service enquiry',
        'Pre despatch order enquiry',
        'Post despatch delivery enquiry',
        'Post despatch returns enquiry',
        'Post despatch item damaged',
        'Technical enquiry',
        'Post despatch general enquiry',
    ]

    for form in tqdm(ticket_forms, leave=True):
        filename = time.strftime("%d_%m_%Y") + '_zendesk_tickets_' + str(form) + '.json'
        extract_tickets(zenpy, start_date, end_date, filename, form=form)
        stage_and_load_tickets(conn, s3, filename)

    delete_duplicates(conn)
    conn.close()


if __name__ == "__main__":
    etl()
