import datetime as dt
import openai
import pinecone
from pinecone.core.client.model.query_response import QueryResponse
import pandas as pd
import json
# from config import settings

# openaiKey = settings.openaiKey
# openaiOrg = settings.openaiOrg
# pineconeKey = settings.pineconeKey
# pineconeEnv = settings.pineconeEnv

openaiKey = "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"
openaiOrg = "org-g13V7ruXHpR7Ez8Rg0hk9Yw0"
pineconeKey = "c1f01425-96ff-4544-97ef-0ba4e22d3d40"
pineconeEnv = "us-east1-gcp"

unfoldProjects = [
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


openai.organization = openaiOrg
# get this from top-right dropdown on OpenAI under organization > settings
openai.api_key = openaiKey
# get API key from top-right dropdown on OpenAI website

# openai.Engine.list()  # check we have authenticated

MODEL = "text-embedding-ada-002"

res = openai.Embedding.create(
    input=[
        "Sample document text goes here",
        "there will be several phrases in each batch"
    ], engine=MODEL
)
embeds = [record['embedding'] for record in res['data']]


projectName = unfoldProjects[0]['ProjectName']
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
# time.sleep(30)


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


# Completion/Summarization/answering davinci function
def complete(prompt):
    # query text-davinci-003
    res = openai.Completion.create(
        engine='text-davinci-003',
        prompt=prompt,
        temperature=0,
        max_tokens=512,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )
    return res['choices'][0]['text'].strip()

def get_insights(preQuestion,query,preFormat,DateFilter):
    customRangeStart = extract_dates(DateFilter)[0] # else '2022-10-01' if custom
    customRangeEnd = extract_dates(DateFilter)[1] # else '2023-01-15' if custom

    # create the query embedding
    xq = openai.Embedding.create(input=query, engine=MODEL)['data'][0]['embedding']
    # query, returning the top 5 most similar results
    res = index.query([xq], top_k=100, include_metadata=True)

    # Assume `res` is the original QueryResponse object
    filtered_matches = [match for match in res.matches if match.score > 0.82]
    filtered_res = QueryResponse(matches=filtered_matches, namespace=res.namespace)

    # The response from Pinecone includes our original text in the `metadata` field, let's print out the `top_k` most similar questions and their respective similarity scores.
    resultIDs = []
    for match in filtered_res['matches']:
        # resultIDs.append(int(match['id']))
        resultIDs.append(match['metadata']['text'])
    #     print(f"{match['id']} | {match['score']:.2f} | {match['metadata']['text']}")

    reviews_df = pd.read_parquet('/Users/princeharshan/Fast API/app/ContactsAllSourcesMerged.parquet.gzip')

    # contextualDF = reviews_df.loc[reviews_df.index.isin(resultIDs)].copy()
    
    contextualDF = reviews_df.loc[reviews_df['Contact body'].isin(resultIDs)].copy()

    # convert timestamp to datetime object
    contextualDF.loc[:, 'timestamp'] = pd.to_datetime(contextualDF['timestamp'])
    contextualDF = contextualDF[(contextualDF['timestamp'] >= customRangeStart) & (contextualDF['timestamp'] <= customRangeEnd)]
    context = contextualDF['Contact body'].to_list()

    # build our prompt with the retrieved contexts included
    prompt_start = (
        '''Answer the question in '''+preFormat.lower()+'''format based on the context below. If the question cannot be answered using the context provided, answer with "None or not enough information"\n\n'''+
        "Context:\n"
    )
    prompt_end = (
        f"\n\nQuestion: "+preQuestion+query+"? Describe your answer.\nAnswer:")

    query_with_contexts = prompt_start+str(context)+prompt_end

     # print(complete(query_with_contexts))
     # print(contextualDF.to_json(orient='records'))

    # convert the dataframe to a json object
    json_object = contextualDF.to_json(orient='records')

    # create a dictionary with the key as the string you want to append
    json_dict = {complete(query_with_contexts): json.loads(json_object)}

    # convert the dictionary to a JSON string
    json_string = json.dumps(json_dict)

    return json_string