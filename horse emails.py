#Imports
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import warnings
from datetime import datetime
import datetime
import re
warnings.filterwarnings('ignore')

#Start and end date for filtering users who started trials
start_date = 20201007
end_date = 20201013

#Function to import data from bigquery
def get_table():
    print("Pulling in table")

    #Sling credentials to big query
    credentials = service_account.Credentials.from_service_account_file(
        'key.json')

    client = bigquery.Client(credentials= credentials, project='thankful-68f22')

    query_job = client.query(
    
    #Pull in table
    """
    SELECT *
    FROM `thankful-68f22.analytics_198686154.events_*`
    """
    )

    #Convert object to df
    results = query_job.result()
    df = results.to_dataframe()
    print("Table imported")
    return df


#Function to isolate users who started the trial from certain dates and export their data 
def user_peeper(idFrame):
    
    global start_date
    global end_date
    
    #Convert date (imports as string) to int for comparisons
    idFrame['event_date'] = idFrame['event_date'].astype(int)
    
    #Filter df to those who started a trial within a certain date range specified above
    ids_within_date_range = idFrame[(idFrame['event_name'] == 'user_started_trial')
                            & ((idFrame['event_date'] >= start_date) &(idFrame['event_date'] <= end_date))]
    
    #Cop those IDs
    trial_ids = list(ids_within_date_range['user_pseudo_id'])
    
    #Loop through the IDs and filter df down to their events and write out to csv
    for i in trial_ids:
    
        idFrame_by_trial_ids = idFrame[idFrame['user_pseudo_id'] ==  i].sort_values(by=['user_pseudo_id','event_timestamp'], ascending = False)

        pd.DataFrame(idFrame_by_trial_ids).to_csv("events" + "_" + i + ".csv")
        print("Extracted events for user " + str(i))


#Run function to obtain table
idFrame = get_table()
#Run function to extract data on certain users who completed trials
user_peeper(idFrame)