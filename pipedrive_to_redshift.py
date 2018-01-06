#ETL script to get pipedrive events from it's API and load them into Redshift

def bucket_to_warehouse(file):
    copycommand = """ 
    DELETE FROM pipedrive.leads; 
    COPY pipedrive.leads 
    FROM 's3://csv-repository/{}'
    CREDENTIALS 'aws_access_key_id=XXX;aws_secret_access_key=XXX'
    CSV
    DELIMITER ',';
    VACUUM pipedrive.leads;
    ANALYZE pipedrive.leads;""".format(file)

    try:
        rs_cursor.execute(copycommand)
    except Exception as e:
        print (e)
    else:    
        pass

if __name__ == "__main__":
    
    #Extract it
    from pipedrive import Pipedrive
    pipedrive = Pipedrive('pipedrive_key')
    response = pipedrive.deals() 
    results = response['data']
    
    #Transform it
    import pandas as pd
    from datetime import date, timedelta, datetime
    import time
    activities = pd.DataFrame(results)
    activities = activities.drop('activities_count', axis=1) #This column does not seem to be properly populated. Getting rid of it.
    activities = activities.drop('probability', axis=1) #This column was added to the api after the creation of the script leading to a bug - just remove.
    activities['etl_tstamp'] = datetime.strftime(date.today(),"%Y-%m-%d %H:%M:%S") #Add ETL timestamp
    activities['last_activity_id'] = activities['last_activity_id'].fillna(0) #Get rid of NaNs.
    activities['last_activity_id'] = activities['last_activity_id'].apply(lambda x: int(x)) #Turn floats to integers. 
    activities['next_activity_id'] = activities['next_activity_id'].fillna(0) #Get rid of NaNs.
    activities['next_activity_id'] = activities['next_activity_id'].apply(lambda x: int(x)) #Turn floats to integers.
    
    #Stage it
    file_name = time.strftime("%Y_%m_%d") + '_pipedrive_activities.csv'
    activities.to_csv(file_name, sep=',', encoding='utf-8', index=False, header=False)
    
    import boto3
    s3 = boto3.client('s3'
                       ,aws_access_key_id='XXX'
                       ,aws_secret_access_key='XXX'
                       ,region_name='eu-west-1')
    s3.upload_file(file_name,'csv-repository',file_name)

    import os
    os.remove(file_name)
    
    #Load it
    import psycopg2
    redshift = "redshift_end_point"
    rs_pgconn = psycopg2.connect(redshift)
    rs_pgconn.set_session(autocommit=True)
    rs_cursor = rs_pgconn.cursor()
    bucket_to_warehouse(file_name)
    rs_pgconn.close()
