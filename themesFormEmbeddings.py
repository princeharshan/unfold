import numpy as np
import pandas as pd
from .app.config import settings
from datetime import datetime
import schedule
import time


def main():

    # load data
    datafile_path = "UserFeedbackEmbeddings.csv"

    df = pd.read_csv(datafile_path)
    df["embedding"] = df.embedding.apply(eval).apply(np.array)  # convert string to numpy array
    matrix = np.vstack(df.embedding.values)
    # matrix.shape

    from sklearn.cluster import KMeans

    n_clusters = 9

    kmeans = KMeans(n_clusters=n_clusters, init="k-means++", random_state=42, n_init=10)
    kmeans.fit(matrix)
    labels = kmeans.labels_
    df["Cluster"] = labels

    # df.groupby("Cluster").rating.mean().sort_values()

    import openai

    # openaiKey = "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"
    openaiKey = settings.openaiKey
    openaiOrg = settings.openaiOrg

    openai.organization = openaiOrg
    # get this from top-right dropdown on OpenAI under organization > settings
    openai.api_key = openaiKey

    import pandas as pd
    import json
        
    # quickClusters = []
    rev_per_cluster = 5

    # create an empty dataframe with the necessary columns
    quickClustersDF = pd.DataFrame(columns=['Themes', 'Channel', 'User ID', 'Contact ID', 'Contact body'])

    for i in range(n_clusters):
        theme = f"Theme {i+1}: "
        reviews = "\n".join(
            df[df.Cluster == i]['Contact body']
            .str.replace("\n\nContent: ", ":  ")
            .sample(rev_per_cluster, random_state=42)
            .values
        )

        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=f'Describe in 75 characters or less what the following customer reviews have in common.\n\nCustomer reviews:\n"""\n{reviews}\n"""\n\nTheme:',
            temperature=0,
            max_tokens=64,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )

        theme_desc = response["choices"][0]["text"].replace("\n", "")
    #     quickClusters.append(theme_desc)

        sample_cluster_rows = df[df.Cluster == i].sample(rev_per_cluster, random_state=42)
        for j in range(rev_per_cluster):
            row = [theme_desc, sample_cluster_rows['Channel'].values[j], sample_cluster_rows['User ID'].values[j], sample_cluster_rows['Contact ID'].values[j], sample_cluster_rows['Contact body'].values[j]]
            quickClustersDF.loc[len(quickClustersDF)] = row

    quickClustersJSON = json.loads(quickClustersDF.sample(9).to_json(orient='records'))

    quickClustersDF['timestamp'] = datetime.today().strftime('%Y-%m-%d')

    quickClustersDF.to_parquet('quickClustersDF.parquet.gzip', compression='gzip')

# Schedule to run the above function every day
schedule.every().day.at("08:00").do(main)

while 1:
    schedule.run_pending()
    time.sleep(90)