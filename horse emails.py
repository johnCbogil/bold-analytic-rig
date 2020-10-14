#!/usr/bin/env python
# coding: utf-8

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

def get_ids():
    print("getting IDs")

    #Sling credentials to big queery
    credentials = service_account.Credentials.from_service_account_file(
        'key.json')

    client = bigquery.Client(credentials= credentials, project='thankful-68f22')
    query_job = client.query(
    """
    SELECT DISTINCT user_pseudo_id
    FROM `thankful-68f22.analytics_198686154.events_*`
    WHERE event_name = 'user_started_trial'
    AND event_date BETWEEN '20201007' AND '20201013' 
    """
    )

    #Convert object to df
    results = query_job.result()
    df = results.to_dataframe()
    print("finished getting IDs")
    return df


def ripEventsForID(id):
    print("ripping id")

    credentials = service_account.Credentials.from_service_account_file(
        'key.json')

    client = bigquery.Client(credentials= credentials, project='thankful-68f22')
    query_job = client.query(
    """
    SELECT *
    FROM `thankful-68f22.analytics_198686154.events_*`
    WHERE user_pseudo_id = '{}'
    ORDER BY event_timestamp DESC
    """.format(id)
    )

    #Convert object to df
    results = query_job.result()
    df = results.to_dataframe()
    print("writing to csv")
    file_name = "events+%s.csv" %(id)
    df.to_csv(file_name)


idFrame = get_ids()
ids = idFrame["user_pseudo_id"].values.tolist()

for id in ids:
    ripEventsForID(id)
