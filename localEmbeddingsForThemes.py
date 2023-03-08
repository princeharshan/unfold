import openai
from .app.config import settings
import pandas as pd
import tiktoken
import datetime
from openai.embeddings_utils import get_embedding
import schedule
import time

openaiKey = settings.openaiKey
openaiOrg = settings.openaiOrg

openai.organization = openaiOrg
openai.api_key = openaiKey

# embedding model parameters
embedding_model = "text-embedding-ada-002"
embedding_encoding = "cl100k_base"  # this the encoding for text-embedding-ada-002
max_tokens = 8000  # the maximum for text-embedding-ada-002 is 8191


def main():
    # load & inspect dataset
    input_datapath = "app/ContactsAllSourcesMerged.parquet.gzip"  # to save space, we provide a pre-filtered dataset
    df = pd.read_parquet(input_datapath)
    df = df[["Channel","Contact ID","User ID","rating","timestamp","Contact body"]]
    # df = df.dropna()
    # df.head(2)

    df = df[df['timestamp'] >= datetime.datetime.now() - pd.to_timedelta("90day")]

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # convert the datetime values to Unix timestamps
    df['timestamp'] = df['timestamp'].apply(lambda x: int(x.timestamp()))

    # subsample to 1k most recent reviews and remove samples that are too long
    top_n = 1000
    df = df.sort_values("timestamp").tail(top_n * 2)  # first cut to first 2k entries, assuming less than half will be filtered out
    df.drop("timestamp", axis=1, inplace=True)

    encoding = tiktoken.get_encoding(embedding_encoding)

    # omit reviews that are too long to embed
    # df["n_tokens"] = df['Contact body'].apply(lambda x: len(encoding.encode(x)))
    df["n_tokens"] = df['Contact body'].apply(lambda x: len(encoding.encode(str(x)) if isinstance(x, (str, bytes)) else b''))

    df = df[df.n_tokens <= max_tokens].tail(top_n)
    # len(df)

    # Ensure you have your API key set in your environment per the README: https://github.com/openai/openai-python#usage

    # This may take a few minutes
    df["embedding"] = df['Contact body'].apply(lambda x: get_embedding(x, engine=embedding_model))
    df.to_csv("UserFeedbackEmbeddings.csv")

# Schedule to run the above function every day
schedule.every().day.at("07:00").do(main)

while 1:
    schedule.run_pending()
    time.sleep(90)    