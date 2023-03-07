# # Twitter social mentions

import tweepy
import pandas as pd
from datetime import datetime, timedelta
from .config import settings
import os
import datetime as dt
import schedule
import time

# #### Environment Vars

# Twitter Creds
twitterAPIKey = settings.twitterAPIKey
twitterAPISecret = settings.twitterAPISecret
twitterBearerToken = settings.twitterBearerToken
twitterAccessToken = settings.twitterAccessToken
twitterAccessTokenSecret = settings.twitterAccessTokenSecret


# Twitter API authentication
auth = tweepy.OAuthHandler(twitterAPIKey, twitterAPISecret)
auth.set_access_token(twitterAccessToken, twitterAccessTokenSecret)
api = tweepy.API(auth)


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

def main():
    # Get tweets from the past X days and not the current trading day
    since_date = datetime.now() - timedelta(days = 30)
    until_date = datetime.now()

    # Create an empty dataframe to store tweets
    tweets_df = pd.DataFrame(columns=['Ticker', 'Username', 'Tweet ID', 'Timestamp', 'Text'])
    request_count = 0

    for term in searchTerms:
        searchParam = term
        
        for page in tweepy.Cursor(api.search_tweets, q = searchParam+" -filter:retweets"+" since:"+since_date.strftime("%Y-%m-%d")+" until:"+until_date.strftime("%Y-%m-%d"), lang="en", tweet_mode='extended').pages():
            for tweet in page:
                tweets_df = pd.concat([tweets_df,pd.DataFrame({'Ticker': searchParam,
                                'Username': tweet.user.screen_name,
                                'Tweet ID': tweet.id,
                                'Timestamp': tweet.created_at,
                                'Retweet Count': tweet.retweet_count,
                                'Like Count': tweet.favorite_count,
                                'Text': tweet.full_text},index=[0])])
                request_count += 1
                # Check the number of requests made
                if request_count % 500 == 0:
                    # Wait for 15 minutes before making more requests
                    print("Waiting for 15 minutes before making more requests...")
                    time.sleep(900)

    # Some transform                
    tweets_df['Text'] = tweets_df['Text'].str.replace('\$', '\\$', regex=True).str.replace('\n', '<br>', regex=True)
    tweets_df['rating'] = tweets_df['Retweet Count'] + tweets_df['Like Count']
    tweets_df['Channel'] = 'Twitter'
    tweets_df['Timestamp'] = pd.to_datetime(tweets_df['Timestamp']).dt.tz_localize(None)
    tweets_df = tweets_df[['Channel','Tweet ID','Username','rating','Timestamp','Text']]
    tweets_df.columns = ['Channel','Contact ID','User ID','rating','timestamp','Contact body']
    tweets_df.drop_duplicates('Contact ID', inplace = True)

    # Removing repeating indexes
    tweets_df.reset_index(inplace=True)
    tweets_df.drop(columns='index',inplace=True)

    SOTLogs = tweets_df.groupby('Channel').apply(lambda x: x.loc[x['timestamp'].idxmax()]).reset_index(drop=True)
    SOTLogs['Last fetch'] = dt.datetime.now()

    if os.path.isfile('SOTLogs.csv'):
        differential = pd.read_csv('SOTLogs.csv')
        differential = differential[~differential['Channel'].isin(SOTLogs['Channel'])]
        differential = pd.concat([SOTLogs,differential])
        differential.to_csv('SOTLogs.csv', index=False)
    else:
        SOTLogs.to_csv('SOTLogs.csv', index=False)

    # Create output parquet if doesn't already exist and only update with new records

    filename = 'tweets_df.parquet.gzip'

    if os.path.isfile(filename):
        # If file already exists, read it into a DataFrame and append new rows
        existing_data = pd.read_parquet(filename)
        updated_data = pd.concat([existing_data, tweets_df], ignore_index=True)
        updated_data.drop_duplicates(subset='Contact ID', keep='last', inplace=True)
    else:
        # If file doesn't exist, just use the new data
        updated_data = tweets_df

    # Write the updated data to the parquet file
    updated_data.to_parquet(filename, compression='gzip')


# Schedule to run the above function every day
schedule.every().day.at("01:00").do(main)

while 1:
    schedule.run_pending()
    time.sleep(90)