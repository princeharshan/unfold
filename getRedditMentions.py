# # Reddit social mentions

import pandas as pd
from datetime import datetime
import datetime as dt
import requests
import os
import schedule
import time
from .config import settings

# #### Environment Vars
# Reddit Creds

redditClientID = settings.redditClientID
redditClientSecret = settings.redditClientSecret
redditUserAgent = settings.redditUserAgent
redditUsername = settings.redditUsername
redditPassword = settings.redditPassword

# Reddit OAuth

# note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
auth = requests.auth.HTTPBasicAuth(redditClientID, redditClientSecret)

# here we pass our login method (password), username, and password
data = {'grant_type': 'password',
        'username': redditUsername,
        'password': redditPassword}

# setup our header info, which gives reddit a brief description of our app
headers = {'User-Agent': redditUserAgent}

# send our request for an OAuth token
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

# convert response to JSON and pull access_token value
TOKEN = res.json()['access_token']

# add authorization to our headers dictionary
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

# while the token is valid (~2 hours) we just add headers=headers to our requests
requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)


# #### Client Input Params

bravokitProjects = [
  {
    "ProjectName": "DasherDirect",
    "iOSAppURL": "https://apps.apple.com/us/app/dasherdirect-by-payfare/id1501107740",
    "AndroidAppURL": "https://play.google.com/store/apps/details?id=com.payfare.doordash&hl=en_CA&gl=US",
    "Keywords": ["DasherDirect","Dasher Direct"]
  },
  {
    "ProjectName": "MyFitnessPal",
    "iOSAppURL": "https://apps.apple.com/us/app/myfitnesspal/id341232718",
    "AndroidAppURL": "https://play.google.com/store/apps/details?id=com.myfitnesspal.android&hl=en_US&gl=US",
    "Keywords": ["MyFitnessPal"]
  }
]

searchTerms = bravokitProjects[0]['Keywords']

keyword = '"+OR+"'.join(searchTerms)
keyword = '"' + keyword + '"'


# initialize dataframe and parameters for pulling data in loop
data = pd.DataFrame()
params = {'limit': 100}


# # <i>Direct Keywords not matching e.g. redd.it/zutwp5


def main():
    # Checking for the last entry by channel in logs to pull new reviews after last review
    SOTLog_path = 'SOTLogs.csv'
    if os.path.exists(SOTLog_path):
        SOTLog = pd.read_csv('SOTLogs.csv')
        max_timestamp = pd.to_datetime(SOTLog[SOTLog['Channel'].str.startswith('r/')]['timestamp']).max()
        if not pd.isnull(max_timestamp):
            queryParams = '''&sort=new&t=all&t=week''' # sort & past week
        else:
            queryParams = '''&sort=new&t=all''' # sort & all-time
    else:
        queryParams = '''&sort=new&t=all''' # sort & all-time


    # Set up the headers and parameters for the request
    params = {"limit": 100}

    # Create an empty list to store the results
    data = []


    # Loop through 3 times (returning 1K posts)
    for i in range(3):
        # Add the keyword to the 'q' parameter of the request URL
        url = f"https://oauth.reddit.com/search/?q={keyword}{queryParams}"

        # Make the request
        res = requests.get(url, headers=headers, params=params)
        
        # Get the JSON data from the response
        json_data = res.json()

        # If there are no matching results, skip to the next iteration
        if len(json_data['data']['children']) == 0:
            continue

        # Take the final post (oldest entry)
        last_post = json_data['data']['children'][-1]

        # Update the 'after' parameter to get the next page of results
        params['after'] = last_post['data']['name']

        # Append the results to the 'data' list
        data.extend(json_data['data']['children'])

    # Convert the 'data' list to a Pandas dataframe
    redditPosts = pd.DataFrame([post['data'] for post in data])


    pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)


    # redditPosts['over_18'] == 


    redditPosts['created'] = redditPosts['created'].apply(datetime.fromtimestamp)
    redditPosts['subreddit'] = 'r/' + redditPosts['subreddit'].astype(str)
    redditPosts['Contact ID'] = 'redd.it/' + redditPosts['id']
    redditPosts['Contact body'] = redditPosts['title'] + '-' + redditPosts['selftext']


    redditPosts = redditPosts[['subreddit','Contact ID','author','ups','created','Contact body']]
    redditPosts.columns = ['Channel','Contact ID','User ID','rating','timestamp','Contact body']


    redditPosts.drop_duplicates('Contact ID', inplace = True)


    # Removing repeating indexes
    redditPosts.reset_index(inplace=True)
    redditPosts.drop(columns='index',inplace=True)

    # Print source of truth logs
    SOTLogs = redditPosts.groupby('Channel').apply(lambda x: x.loc[x['timestamp'].idxmax()]).reset_index(drop=True)
    SOTLogs['Last fetch'] = dt.datetime.now()

    if os.path.isfile('SOTLogs.csv'):
        differential = pd.read_csv('SOTLogs.csv')
        differential = differential[~differential['Channel'].isin(SOTLogs['Channel'])]
        differential = pd.concat([SOTLogs,differential])
        differential.to_csv('SOTLogs.csv', index=False)
    else:
        SOTLogs.to_csv('SOTLogs.csv', index=False)


    # Create output parquet if doesn't already exist and only update with new records
    filename = 'redditPosts.parquet.gzip'

    if os.path.isfile(filename):
        # If file already exists, read it into a DataFrame and append new rows
        existing_data = pd.read_parquet(filename)
        updated_data = pd.concat([existing_data, redditPosts], ignore_index=True)
        updated_data.drop_duplicates(subset='Contact ID', keep='last', inplace=True)
    else:
        # If file doesn't exist, just use the new data
        updated_data = redditPosts

    # Write the updated data to the parquet file
    updated_data.to_parquet(filename, compression='gzip')


# Schedule to run the above function every day
schedule.every().day.at("01:00").do(main)

while 1:
    schedule.run_pending()
    time.sleep(90)