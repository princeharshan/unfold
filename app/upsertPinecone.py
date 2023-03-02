import openai
import pinecone
import os
from .config import settings
from tqdm.auto import tqdm
import pandas as pd
import datetime as dt
import schedule
import time

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

openaiKey = settings.openaiKey
openaiOrg = settings.openaiOrg
pineconeKey = settings.pineconeKey
pineconeEnv = settings.pineconeEnv

openai.organization = openaiOrg
# get this from top-right dropdown on OpenAI under organization > settings
openai.api_key = openaiKey
# get API key from top-right dropdown on OpenAI website

# openai.Engine.list()  # check we have authenticated


def extract_dates(text):
    today = dt.date.today()
    start_date = None
    end_date = None

    if "today" in text.lower():
        start_date = today
        end_date = today

    elif "yesterday" in text.lower():
        start_date = today - dt.timedelta(days=1)
        end_date = start_date

    elif "this week" in text.lower():
        start_date = today - dt.timedelta(days=today.weekday())
        end_date = start_date + dt.timedelta(days=6)

    elif "last week" in text.lower():
        start_date = today - dt.timedelta(days=today.weekday() + 7)
        end_date = start_date + dt.timedelta(days=6)

    elif "this month" in text.lower():
        start_date = today.replace(day=1)
        end_date = today.replace(day=dt.date.today().day)

    elif "last month" in text.lower():
        start_date = (today.replace(day=1) - dt.timedelta(days=today.day)).replace(day=1)
        end_date = (today.replace(day=1) - dt.timedelta(days=1))

    elif "last 7 days" in text.lower():
        start_date = today - dt.timedelta(days=6)
        end_date = today

    elif "last 2 weeks" in text.lower():
        start_date = today - dt.timedelta(days=13)
        end_date = today

    elif "last 1 month" in text.lower():
        start_date = today - dt.timedelta(days=29)
        end_date = today

    elif "last 3 months" in text.lower():
        start_date = today - dt.timedelta(days=90)
        end_date = today

    elif "last 6 months" in text.lower():
        start_date = today - dt.timedelta(days=181)
        end_date = today

    elif "last 1 year" in text.lower():
        start_date = today - dt.timedelta(days=364)
        end_date = today

    elif "ytd" in text.lower():
        start_date = today.replace(month=1, day=1)
        end_date = today

    if start_date and end_date:
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    else:
        return None
    

# We can now create embeddings with the OpenAI Ada similarity model like so:

MODEL = "text-embedding-ada-002"

res = openai.Embedding.create(
    input=[
        "Sample document text goes here",
        "there will be several phrases in each batch"
    ], engine=MODEL
)

embeds = [record['embedding'] for record in res['data']]

# Next, we initialize our index to store vector embeddings with Pinecone.

projectName = bravokitProjects[0]['ProjectName']
projectIndex = projectName.lower()

# initialize connection to pinecone (get API key at app.pinecone.io)
pinecone.init(
    api_key = pineconeKey,
    environment = pineconeEnv
)
# check if projectIndex index already exists (only create index if not)
if projectIndex not in pinecone.list_indexes():
    pinecone.create_index(projectIndex, dimension=len(embeds[0]))
# connect to index
index = pinecone.Index(projectIndex)
time.sleep(30)


def main():
    # Load the app store review data
    # reviews_df = pd.read_csv("customer_contacts.csv")
    reviews_df = pd.read_parquet('differential_data.parquet.gzip')
    reviews_df['timestamp'] = pd.to_datetime(reviews_df['timestamp']).dt.strftime('%d-%b-%Y')
    reviews_df['Channel'] = reviews_df['Channel'].replace({'App Store': 'iOS', 'Google Play': 'Android'})

    # data = Dataset.from_pandas(reviews_df)
    review_texts = list(reviews_df["Contact body"])

    # Then we create a vector embedding for each phrase using OpenAI, and `upsert` the ID, vector embedding, and original text for each phrase to Pinecone.

    filename = 'ContactsAllSourcesMerged.parquet.gzip'

    if os.path.isfile(filename):
        ContactsAllSourcesMerged = pd.read_parquet('ContactsAllSourcesMerged.parquet.gzip')
        offset = len(ContactsAllSourcesMerged)-1
    else:
        offset = 0


    from tqdm.auto import tqdm

    count = 0  # we'll use the count to create unique IDs
    batch_size = 32  # process everything in batches of 32
    for i in tqdm(range(0, len(review_texts), batch_size)):
        # set end position of batch
        i_end = min(i+batch_size, len(review_texts))
        # get batch of lines and IDs
        lines_batch = review_texts[i: i+batch_size]
        ids_batch = [str(n + offset) for n in range(i, i_end)]
    #     ids_batch = [str(n) for n in range(i, i_end)]
        # create embeddings
        res = openai.Embedding.create(input=lines_batch, engine=MODEL)
        embeds = [record['embedding'] for record in res['data']]
        # prep metadata and upsert batch
        meta = [{'text': line} for line in lines_batch]
        to_upsert = zip(ids_batch, embeds, meta)
        # upsert to Pinecone
        index.upsert(vectors=list(to_upsert))

# Schedule to run the above function every day

schedule.every().day.at("07:00").do(main)

while 1:
    schedule.run_pending()
    time.sleep(90)