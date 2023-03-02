# # Get all app review

# !pip install google_play_scraper
# !pip install app_store_scraper
# !pip install --upgrade google-play-scraper
# !pip install pyopenssl


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

def extract_iOS_app_info(url):
    # Check if the URL is for the App Store
    if "apps.apple.com" in url:
        # Split the URL into its components using "/"
        components = url.split("/")
        # Extract the region, app ID, and app name from the components
        region = components[3]
        app_id = components[-1].split("id")[-1]
        app_name = components[-2]
    else:
        # If the URL is not for the App Store, return None values
        region = None
        app_name = None
        app_id = None
    # Return the extracted values as a tuple
    return region, app_name, app_id


def extract_Android_app_info(url):
    # Check if the URL is for Google Play
    if "play.google.com" in url:
        # Split the URL into its components using "/"
        components = url.split("/")
        # Extract the package name, language, and country code from the URL parameters
        params = components[-1].split("?")[1]
        params_dict = dict(param.split("=") for param in params.split("&"))
        package_name = params_dict.get("id")
        language = params_dict.get("hl", "").split("-")[0]
        country = params_dict.get("gl", "")
    else:
        # If the URL is not for Google Play, return None values
        package_name = None
        language = None
        country = None
    # Return the extracted values as a tuple
    return package_name, language, country


iOSProps = extract_iOS_app_info(bravokitProjects[0]['iOSAppURL'])
AndroidProps = extract_Android_app_info(bravokitProjects[0]['AndroidAppURL'])


from google_play_scraper import app, Sort, reviews_all
from app_store_scraper import AppStore
import pandas as pd
import numpy as np
import json, os
import datetime as dt
import os
import schedule
import time

def main():
    # Checking for the last entry by channel in logs to pull new reviews after last review

    SOTLog_path = 'SOTLogs.csv'
    if os.path.exists(SOTLog_path):
        SOTLog = pd.read_csv('SOTLogs.csv')
        max_timestamp = pd.to_datetime(SOTLog[SOTLog['Channel'] == 'iOS']['timestamp']).max()
        if not pd.isnull(max_timestamp):
            max_timestamp = max_timestamp.to_pydatetime() - dt.timedelta(days=1) # moving it back by one day to be safe
            a_reviews = AppStore(iOSProps[0], iOSProps[1], iOSProps[2])
            a_reviews.review(after=max_timestamp, sleep=2)
        else:
            a_reviews = AppStore(iOSProps[0], iOSProps[1], iOSProps[2])
            a_reviews.review(sleep=2)
    else:
        a_reviews = AppStore(iOSProps[0], iOSProps[1], iOSProps[2])
        a_reviews.review(sleep=2)


    g_reviews = reviews_all(
            AndroidProps[0],
            sleep_milliseconds = 15, # defaults to 0
            lang=AndroidProps[1], # defaults to 'en'
            country=AndroidProps[2], # defaults to 'us'
            sort=Sort.NEWEST, # defaults to Sort.MOST_RELEVANT
        )


    g_df = pd.DataFrame(np.array(g_reviews),columns=['review'])
    g_df2 = g_df.join(pd.DataFrame(g_df.pop('review').tolist()))

    g_df2.drop(columns={'userImage', 'reviewCreatedVersion'},inplace = True)
    g_df2.rename(columns= {'score': 'rating','userName': 'user_name', 'reviewId': 'review_id', 'content': 'review_description', 'at': 'review_date', 'replyContent': 'developer_response', 'repliedAt': 'developer_response_date', 'thumbsUpCount': 'thumbs_up'},inplace = True)
    g_df2.insert(loc=0, column='source', value='Google Play')
    g_df2.insert(loc=3, column='review_title', value=None)
    g_df2['laguage_code'] = AndroidProps[1]
    g_df2['country_code'] = AndroidProps[2]


    a_df = pd.DataFrame(np.array(a_reviews.reviews),columns=['review'])
    a_df2 = a_df.join(pd.DataFrame(a_df.pop('review').tolist()))

    a_df2.drop(columns={'isEdited'},inplace = True)
    a_df2.insert(loc=0, column='source', value='App Store')
    a_df2['developer_response_date'] = None
    a_df2['thumbs_up'] = None
    a_df2['laguage_code'] = 'en'
    a_df2['country_code'] = iOSProps[0]
    a_df2.insert(loc=1, column='review_id', value='')
    a_df2.rename(columns= {'review': 'review_description','userName': 'user_name', 'date': 'review_date','title': 'review_title', 'developerResponse': 'developer_response'},inplace = True)
    a_df2 = a_df2.where(pd.notnull(a_df2), None)
    a_df2['review_id'] = a_df2['review_id']+a_df2['user_name']


    result = pd.concat([g_df2,a_df2])


    result.drop(['developer_response', 'developer_response_date', 'thumbs_up', 'laguage_code','country_code'], axis=1, inplace=True)


    # ## Transform and normalize


    # Fill the NaN values in col1 with an empty string
    result['review_title'] = result['review_title'].fillna('')

    # Create a new column that concatenates the values of col1 and col2 using the apply method
    result['Contact body'] = result.apply(lambda row: row['review_title'] + ' ' + row['review_description'] if row['review_description'] is not None else row['review_description'], axis=1)

    result.drop(['review_title','review_description'], axis=1, inplace=True)
    result.columns = ['Channel','Contact ID','User ID','rating','timestamp','Contact body']
    result['Channel'] = result['Channel'].replace({'App Store': 'iOS', 'Google Play': 'Android'})
    result['Contact ID'] = pd.concat([result['Channel'], result['Contact ID'].astype(str)], axis=1).apply(':'.join, axis=1)

    # ### Prepping for export & logging

    result.drop_duplicates('Contact ID', inplace = True)

    # Removing repeating indexes
    result.reset_index(inplace=True)
    result.drop(columns='index',inplace=True)


    # Print source of truth logs

    SOTLogs = result.groupby('Channel').apply(lambda x: x.loc[x['timestamp'].idxmax()]).reset_index(drop=True)
    SOTLogs['Contact ID'] = SOTLogs['Contact ID'].apply(lambda x: x.split(':')[1])
    SOTLogs['Last fetch'] = dt.datetime.now()

    if os.path.isfile('SOTLogs.csv'):
        differential = pd.read_csv('SOTLogs.csv')
        differential = differential[~differential['Channel'].isin(SOTLogs['Channel'])]
        differential = pd.concat([SOTLogs,differential])
        differential.to_csv('SOTLogs.csv', index=False)
    else:
        SOTLogs.to_csv('SOTLogs.csv', index=False)

    # Create output parquet if doesn't already exist and only update with new records

    filename = 'result.parquet.gzip'

    if os.path.isfile(filename):
        # If file already exists, read it into a DataFrame and append new rows
        existing_data = pd.read_parquet(filename)
        updated_data = pd.concat([existing_data, result], ignore_index=True)
        updated_data.drop_duplicates(subset='Contact ID', keep='last', inplace=True)
    else:
        # If file doesn't exist, just use the new data
        updated_data = result

    # Write the updated data to the parquet file
    updated_data.to_parquet(filename, compression='gzip')



# Schedule to run the above function every day

schedule.every().day.at("01:00").do(main)

while 1:
    schedule.run_pending()
    time.sleep(90)