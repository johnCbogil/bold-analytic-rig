#!/usr/bin/env python
# coding: utf-8

# In[3]:


#Read in packages
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

email_list = []       #Initialize empty list to store scraped emails

#Sling credentials to big queery
credentials = service_account.Credentials.from_service_account_file(
    'key.json')

project_id = 'thankful-68f22'

client = bigquery.Client(credentials= credentials,project=project_id)

#Pull everything from the app's table
query_job = client.query("""
SELECT * FROM `thankful-68f22.analytics_198686154.events_*` WHERE event_name = 'email_submitted' ORDER BY event_timestamp DESC
 """)
results = query_job.result()


# In[5]:


#Pull table into pandas
df = results.to_dataframe()


# In[159]:


#Make a copy of the df so you dont have to pull everything from gcp back into memory and waste time type
df2 = df


# In[225]:


def horse_emails(table_input):
    
    ### THIS FUNCTION WILL HORSE EMAILS OUT OF YOUR TABLE ###
    ### CREATED BY JOSEPH VORBECK 10/13/1994 - MY BDAY ###
    ### THIS FUNCTION TAKES ONE INPUT:
       ### A DATAFRAME PULLED FROM BIG QUERY THAT CONTAINS AT LEASTTTTT THE EVENT NAME AND EVEN PARAMS FIELDS ###
    ### PRETTY SURE THIS IS GUCCI ###
    
    
    # filtered_df = x[x['event_name'] == 'email_submitted']  #Filters input dataframe to rows in which email actions were taken
    # TODO: FIND WAY TO COUNT DYNAMICALLY
    loop_len = 190 ##range(0, x['event_name'].count())   #Obtains length of dataframe used for loop iterations
    dict_values = list(table_input['event_params'].values) #Obtains values from nested dicts which contain emails
    email_regex = r'[A-z0-9.]+@+[a-z]+[.]+[a-z]+[a-z]'   #Regex pattern to match emails
    global email_list

    # for _ in range(190):
    list_normalized = str(dict_values).replace("'","")  #Replace quotes around emails, just makes it easier
    reggy = re.findall(email_regex, list_normalized) #Match to email
    if reggy:                                 
        email_list += reggy                   #If there is a match put the email in the list
    else:                                     #If not drop a message
        print("No email associated")

    return email_list

# In[227]:


list_of_emails = horse_emails(df2)

print(list_of_emails)

