from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import pandas as pd
import json
from typing import Optional
import pinecone
from .ancillaryFunctions import complete, extract_dates
import openai
from pinecone.core.client.model.query_response import QueryResponse

from .config import settings

openaiKey = settings.openaiKey
openaiOrg = settings.openaiOrg
pineconeKey = settings.pineconeKey
pineconeEnv = settings.pineconeEnv

openai.organization = openaiOrg
# get this from top-right dropdown on OpenAI under organization > settings
openai.api_key = openaiKey
# get API key from top-right dropdown on OpenAI website

# openai.Engine.list()  # check we have authenticated

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

projectName = unfoldProjects[0]['ProjectName']
projectIndex = projectName.lower()

MODEL = "text-embedding-ada-002"


res = openai.Embedding.create(
    input=[
        "Sample document text goes here",
        "there will be several phrases in each batch"
    ], engine=MODEL
)
embeds = [record['embedding'] for record in res['data']]


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

app = FastAPI()

while True:
    try:
        conn = psycopg2.connect(host=settings.PostgreSQLHost,database=settings.PostgreSQLDatabase,user=settings.PostgreSQLUser,password=settings.PostgreSQLPassword,cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        print("Database connection was successful!")
        break
    except Exception as error:
        print("Connecting to daabase failed!")
        print("Error: ", error)
        time.sleep(2)


class insights(BaseModel):
    preQuestion: str
    query: str
    preFormat: str
    DateFilter: str


class project(BaseModel):
    projectName: str
    appStoreURL: Optional[str] = None
    playStoreURL: Optional[str] = None
    keywords: Optional[str] = None
    bubbleID: str


@app.put("/projects/{id}")
async def update_project(id: int, project: project):
    cursor.execute("""UPDATE projects SET name = %s, app_store_url = %s, play_store_url = %s, keywords = %s WHERE id = %s RETURNING *"""
                   , (project.projectName, project.appStoreURL, project.playStoreURL, project.keywords, str(id))) 
    
    updated_project = cursor.fetchone()
    conn.commit()
    
    if update_project == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Project with id: {id} does not exist.")
    
    return {"data": update_project}


@app.get("/projects")
async def get_projects():
    cursor.execute("""Select * from projects""")
    projects = cursor.fetchall()
    print(projects)
    return {"data": projects}


@app.post("/projects", status_code=status.HTTP_201_CREATED)
async def create_project(project:project):
    cursor.execute("""INSERT INTO projects (name, app_store_url, play_store_url, keywords, bubble_project_id) VALUES (%s, %s, %s, %s, %s) RETURNING * """, (project.projectName, project.appStoreURL, project.playStoreURL, project.keywords, project.bubbleID)) 
    new_project = cursor.fetchone()
    conn.commit()
    return {"data": new_project}


@app.get("/insights")
async def get_insights(insights:insights):
    customRangeStart = extract_dates(insights.DateFilter)[0] # else '2022-10-01' if custom
    customRangeEnd = extract_dates(insights.DateFilter)[1] # else '2023-01-15' if custom

    # create the query embedding
    xq = openai.Embedding.create(input=insights.query, engine=MODEL)['data'][0]['embedding']
    # query, returning the top 5 most similar results
    res = index.query([xq], top_k=500, include_metadata=True)

    # Assume `res` is the original QueryResponse object
    filtered_matches = [match for match in res.matches if match.score > 0.82]
    filtered_res = QueryResponse(matches=filtered_matches, namespace=res.namespace)

    # The response from Pinecone includes our original text in the `metadata` field, let's print out the `top_k` most similar questions and their respective similarity scores.
    resultIDs = []
    for match in filtered_res['matches']:
        # resultIDs.append(int(match['id']))
        resultIDs.append(match['metadata']['text'])
    #     print(f"{match['id']} | {match['score']:.2f} | {match['metadata']['text']}")

    # reviews_df = pd.read_parquet('ContactsAllSourcesMerged.parquet.gzip')
    reviews_df = pd.read_parquet('app/ContactsAllSourcesMerged.parquet.gzip')

    # contextualDF = reviews_df.loc[reviews_df.index.isin(resultIDs)].copy()
    contextualDF = reviews_df.loc[reviews_df['Contact body'].isin(resultIDs)].copy()

    # convert timestamp to datetime object
    contextualDF.loc[:, 'timestamp'] = pd.to_datetime(contextualDF['timestamp'])
    contextualDF = contextualDF[(contextualDF['timestamp'] >= customRangeStart) & (contextualDF['timestamp'] <= customRangeEnd)]
    context = contextualDF['Contact body'].to_list()
    context = context[:100]

    # build our prompt with the retrieved contexts included
    prompt_start = (
        '''Answer the question in '''+insights.preFormat.lower()+'''format based on the context below. If the question cannot be answered using the context provided, answer with "None or not enough information"\n\n'''+
        "Context:\n"
    )
    prompt_end = (
        f"\n\nQuestion: "+insights.preQuestion+insights.query+"? Describe your answer.\nAnswer:")

    query_with_contexts = prompt_start+str(context)+prompt_end

    # print(complete(query_with_contexts))
    # print(contextualDF.to_json(orient='records'))

    # convert the dataframe to a json object
    json_object = contextualDF.to_json(orient='records')

    # # create a dictionary with the key as the string you want to append
    # json_dict = {complete(query_with_contexts): json.loads(json_object)}

    # create a dictionary with the key as the string you want to append
    json_dict = {
        "summary": complete(query_with_contexts),
        "values": json.loads(json_object)
    }

    # convert the dictionary to a JSON string
    # json_string = json.dumps(json_dict)

    return json_dict


@app.get("/insightsRefactored")
async def get_insightsRefactored(preQuestion: str, query: str, preFormat: str, DateFilter: str):    
    customRangeStart = extract_dates(DateFilter)[0] # else '2022-10-01' if custom
    customRangeEnd = extract_dates(DateFilter)[1] # else '2023-01-15' if custom

    # create the query embedding
    xq = openai.Embedding.create(input=query, engine=MODEL)['data'][0]['embedding']
    # query, returning the top 5 most similar results
    res = index.query([xq], top_k=500, include_metadata=True)

    # Assume `res` is the original QueryResponse object
    filtered_matches = [match for match in res.matches if match.score > 0.82]
    filtered_res = QueryResponse(matches=filtered_matches, namespace=res.namespace)

    # The response from Pinecone includes our original text in the `metadata` field, let's print out the `top_k` most similar questions and their respective similarity scores.
    resultIDs = []
    for match in filtered_res['matches']:
        # resultIDs.append(int(match['id']))
        resultIDs.append(match['metadata']['text'])
    #     print(f"{match['id']} | {match['score']:.2f} | {match['metadata']['text']}")

    # reviews_df = pd.read_parquet('ContactsAllSourcesMerged.parquet.gzip')
    reviews_df = pd.read_parquet('app/ContactsAllSourcesMerged.parquet.gzip')

    # contextualDF = reviews_df.loc[reviews_df.index.isin(resultIDs)].copy()
    contextualDF = reviews_df.loc[reviews_df['Contact body'].isin(resultIDs)].copy()

    # convert timestamp to datetime object
    contextualDF.loc[:, 'timestamp'] = pd.to_datetime(contextualDF['timestamp'])
    contextualDF = contextualDF[(contextualDF['timestamp'] >= customRangeStart) & (contextualDF['timestamp'] <= customRangeEnd)]
    context = contextualDF['Contact body'].to_list()
    context = context[:100]

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

    # # create a dictionary with the key as the string you want to append
    # json_dict = {complete(query_with_contexts): json.loads(json_object)}

    # create a dictionary with the key as the string you want to append
    json_dict = {
        "summary": complete(query_with_contexts),
        "values": json.loads(json_object)
    }

    # convert the dictionary to a JSON string
    # json_string = json.dumps(json_dict)

    return json_dict


@app.get("/")
async def root():
    return {"message": "Welcome to unfold APIs"}
