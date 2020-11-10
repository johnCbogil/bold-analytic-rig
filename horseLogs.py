#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import warnings
from datetime import timedelta, datetime
import datetime
import re
warnings.filterwarnings('ignore')

start_date = 20201024
end_date = 20201031

def get_ids():
    print("getting IDs")

    #Sling credentials to big queery
    credentials = service_account.Credentials.from_service_account_file('key.json')

    client = bigquery.Client(credentials= credentials, project='thankful-68f22')
    query_job = client.query(
    """
    SELECT DISTINCT user_pseudo_id
    FROM `thankful-68f22.analytics_198686154.events_*`
    WHERE event_name = 'user_started_trial'
    AND event_date BETWEEN '{}' AND '{}' 
    """.format(start_date, end_date)
    )

    #Convert object to df
    results = query_job.result()
    df = results.to_dataframe()
    print("finished getting IDs")
    print(df.shape[0])
    return df

def fetchStartDate(id):
	print("fetching start date")
	credentials = service_account.Credentials.from_service_account_file('key.json')
	client = bigquery.Client(credentials= credentials, project='thankful-68f22')
	query_job = client.query(
    """
    SELECT event_date
    FROM `thankful-68f22.analytics_198686154.events_*`
    WHERE event_name = 'user_started_trial'
    AND user_pseudo_id ='{}'
    """.format(id)
    )
	results = query_job.result()
	df = results.to_dataframe()
	date_time_str = df['event_date'].values[0]

	# convert to date object
	date_object = datetime.datetime.strptime(date_time_str, '%Y%m%d')

	# add 7 days
	end = date_object + timedelta(days=7)

	# convert back to string
	end_string = end.strftime('%Y%m%d')
	print(end_string)

def ripEventsForID(id):
    print("ripping id")

    credentials = service_account.Credentials.from_service_account_file('key.json')

    client = bigquery.Client(credentials= credentials, project='thankful-68f22')
    query_job = client.query(
    """
    SELECT *
    FROM `thankful-68f22.analytics_198686154.events_*`
    WHERE user_pseudo_id = '{}'
    ORDER BY event_timestamp ASC
    """.format(id, start_date, end_date)
    )

    #Convert object to df
    results = query_job.result()
    df = results.to_dataframe()

    completed_trial_count = df['event_name'].str.contains('user_completed_trial').sum()
    if completed_trial_count > 0:
        print("writing to trial completed")
        file_name = "completed/trial_completed+%s.json" %(id)
        result = df.to_json(orient="records")
        parsed = json.loads(result)
        with open(file_name, 'w') as outfile:
            json.dump(parsed, outfile, indent=4)
    else:
        print("writing to trial NOT completed")
        file_name = "not_completed/trial_not_completed+%s.json" %(id)
        result = df.to_json(orient="records")
        parsed = json.loads(result)
        with open(file_name, 'w') as outfile:
            json.dump(parsed, outfile, indent=4)

idFrame = get_ids()
ids = idFrame["user_pseudo_id"].values.tolist()

for id in ids:
    fetchStartDate(id)
