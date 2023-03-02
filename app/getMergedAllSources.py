import pandas as pd
import re
import string
import schedule
import time
import os

# Function to remove embelishments
def remove_words(text):
    words_to_remove = ['actually','basically','completely','super','definitely','entirely','extremely','fully','greatly','highly','incredible','just','mostly','naturally','nearly','particularly','pretty','really','simply','somewhat','strongly','totally','truly','utterly','well','wonderfully','really','actually','extremely','incredibly','unbelievably','utterly','simply','definitely','absolutely','honestly','entirely','wholly','completely','very','too','just','pretty','quite','rather','literally','totally','very','so','too','quite','rather','incredibly','exceedingly','amazingly','utterly','totally','absolutely','definitely','seriously','absolutely','completely','truly','utterly','deeply','absolutely','unbelievably','completely','entirely','totally','totally','absolutely','perfectly','wholly','altogether','radically','extraordinarily','fantastically','incredibly','phenomenally','remarkably','astoundingly','stunningly','miraculously','unusually','surprisingly','awfully','monstrously','vastly','thoroughly','fully','totally','totally','ridiculously','unbelievably','unimaginably','incredibly','impressively','astronomically','tremendously','staggeringly','mind-blowing','mind-blowingly','colossally','stupendously','monumentally','massively','dramatically','overwhelmingly','phenomenally','titanically','colossally','awesomely','astoundingly','terrifically','superbly','grandly','magnificently','splendidly','majestically','majestically','beautifully','impressively','impressive','superbly','marvellously','grandly','gloriously','excellently','fabulously','spectacularly','magnificently','spectacularly','astounding','extraordinary','incredible','amazing','impressive','spectacular','fantastic','magnificent','stunning','superb','fabulous','splendid','glorious','excellent','marvellous','grand','majestic','beautiful','impressive','superb']
    words = text.split()
    words = [word for word in words if word.lower() not in [w.lower() for w in words_to_remove]]
    return " ".join(words)

# Remove twitter urls
def remove_urls(tweet_body):
    # regex to match URLs
    url_pattern = re.compile(r'https?://\S+')

    # remove URLs from the tweet body
    tweet_body = re.sub(url_pattern, '', tweet_body)
    return tweet_body

# Function to Normalize text
def clean_text(column):

    # Keep only alphabets, numbers, periods, and spaces
    column = column.str.replace('[^A-Za-z0-9.$ %()\-+=*"\'/<>!?-]+', '', regex=True)
    
    # Remove leading and trailing whitespaces
    column = column.str.strip()
    
    # Replace multiple whitespaces with a single space
    column = column.str.replace('\s+', ' ', regex=True)
    
    # Remove trailing punctuations
    column = column.str.rstrip(string.punctuation)
    
    # Remove urls
    column = column.apply(remove_urls)
    
#     # Remove words from list
#     column = column.apply(remove_words)
    
    return column

def main():
    tweets_df = pd.read_parquet('tweets_df.parquet.gzip')
    redditPosts = pd.read_parquet('redditPosts.parquet.gzip')
    reviews_df = pd.read_parquet("result.parquet.gzip")

    tweets_df['Contact ID'] = tweets_df['Contact ID'].astype(str)
    latest_data = pd.concat([tweets_df, redditPosts, reviews_df])
    pd.set_option('display.max_colwidth', None)
    latest_data['Contact body'] = clean_text(latest_data['Contact body'])
    # Remove contacts that are <= 3 words long
    latest_data = latest_data.loc[latest_data['Contact body'].str.split().str.len() > 3]

    ### Prepping for export & logging

    latest_data.reset_index(inplace=True)
    latest_data.drop(columns='index',inplace=True)

    filename = 'ContactsAllSourcesMerged.parquet.gzip'

    if os.path.isfile(filename):
        # If file already exists, read it into a DataFrame
        existing_data = pd.read_parquet(filename)
        
        # select only rows from the new dataframe that have a "Contact ID" that is not present in the old dataframe
        differential_data = latest_data[~latest_data['Contact ID'].isin(existing_data['Contact ID'])].dropna()

        # Setting additive indexes to differential_data df incremental from existing_data
        differential_data.reset_index(inplace=True)
        differential_data.drop(columns='index',inplace=True)
        differential_data.index +=len(existing_data)
        differential_data.to_parquet('differential_data.parquet.gzip', compression='gzip')

        # Writing into ContactsAllSourcesMerged for indexed reference data
        ContactsAllSourcesMerged = pd.concat([existing_data,differential_data])
        ContactsAllSourcesMerged.to_parquet(filename, compression='gzip')
        
    else:
        # If file doesn't exist, just use the new data
        differential_data = latest_data
        # Write differential data into new df for indexing with pinecone. Adding additive df indices to already indexed data in pinecone
        differential_data.loc[:, 'Contact ID'] = differential_data['Contact ID'].astype(str)
        differential_data.to_parquet('differential_data.parquet.gzip', compression='gzip')
        differential_data.to_parquet(filename, compression='gzip')

# Schedule to run the above function every day

schedule.every().day.at("06:00").do(main)

while 1:
    schedule.run_pending()
    time.sleep(90)