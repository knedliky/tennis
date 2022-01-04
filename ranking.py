"""
A script to run weekly in order to get updated ATP and WTA rankings to ingress into AWS RDS MySQL Database
"""
import json
import os

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine

# Database information
ENDPOINT = os.getenv("ENDPOINT")
PORT = os.getenv("PORT")
USER = os.getenv("USER")
REGION = os.getenv("REGION")
DBNAME = os.getenv("DBNAME")
PASSWD = os.getenv("PASSWD")

# API endpoint information
urls = [
    "https://sportscore1.p.rapidapi.com/tennis-rankings/atp",
    "https://sportscore1.p.rapidapi.com/tennis-rankings/wta",
]
headers = {
    "x-rapidapi-host": "sportscore1.p.rapidapi.com",
    "x-rapidapi-key": "811228ff06msh50e3dfe9f8a5ecfp1e975ejsn6b992dbfd77b",
}


def get_data(url):
    response = requests.request("GET", url, headers=headers)
    json_data = json.loads(response.text)
    (
        ranking,
        player_id,
        player_first_name,
        player_last_name,
        points,
        tour,
        ranking_date,
    ) = [[] for i in range(7)]

    for rank in json_data["data"]:
        ranking.append(rank["ranking"])
        player_id.append(rank["team"]["id"])
        player_first_name.append(rank["team"]["name_full"].split(", ")[1])
        player_last_name.append(rank["team"]["name_full"].split(", ")[0])
        points.append(rank["points"])
        tour.append(rank["type"])
        ranking_date.append(rank["official_updated_at"])

    return pd.DataFrame(
        np.column_stack(
            [
                ranking,
                player_id,
                player_first_name,
                player_last_name,
                points,
                tour,
                ranking_date,
            ]
        ),
        columns=[
            "ranking",
            "player_id",
            "player_first_name",
            "player_last_name",
            "points",
            "tour",
            "ranking_date",
        ],
    )


# Create DataFrame for ATP and WTA rankings and ingress into Database
ranking = pd.DataFrame(
    columns=[
        "ranking",
        "player_id",
        "player_first_name",
        "player_last_name",
        "points",
        "tour",
        "ranking_date",
    ]
)
for url in urls:
    ranking = ranking.append(get_data(url))
ranking.to_csv("test.csv", index=False)

# Create a connection to the MySQL database, creating a temporary table in order to insert and ignore duplicate values
engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWD}@{ENDPOINT}/{DBNAME}")
ranking.to_sql(con=engine, name="temp_ranking", if_exists="replace", index=False)
connection = engine.connect()
result = connection.execute("INSERT IGNORE INTO ranking SELECT * FROM temp_ranking")
connection.close()
