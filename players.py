"""
A script to run monthly to add and/ or update players in the players table of the database
"""
import json
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from numpy.lib.shape_base import column_stack
from sqlalchemy import create_engine

# DataFrame structure
(
    player_id,
    player_first_name,
    player_last_name,
    country_code,
    country,
    dob,
    plays,
    weight,
    height,
    prize_current_euros,
    prize_total_euros,
    logo,
) = [[] for i in range(12)]


# API endpoint - players by sport ID (Tennis)
url = "https://sportscore1.p.rapidapi.com/sports/2/teams"
headers = {
    "x-rapidapi-host": "sportscore1.p.rapidapi.com",
    "x-rapidapi-key": "811228ff06msh50e3dfe9f8a5ecfp1e975ejsn6b992dbfd77b",
}
querystring = {"page": "1"}
response = requests.request("GET", url, headers=headers, params=querystring)
data = json.loads(response.text)
last_page = data.get("meta").get("last_page")


# Loop over every page and generate new json data
for page in range(last_page - 1):
    querystring["page"] = str(page + 1)
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = json.loads(response.text)

    for player in data.get("data"):
        # Filtering empty players and country teams
        if player is None:
            continue
        if player.get("name_full") is None:
            continue
        if len(player.get("name_full").split(", ")) < 2:
            continue
        player_id.append(player.get("id"))
        player_first_name.append(player.get("name_full").split(", ")[1])
        player_last_name.append(player.get("name_full").split(", ")[0])
        country_code.append(player.get("country_code"))
        country.append(player.get("country"))
        logo.append(player.get("logo"))
        if player.get("details") is None:
            dob.append(None)
            plays.append(None)
            weight.append(None)
            height.append(None)
            prize_current_euros.append(None)
            prize_total_euros.append(None)
        else:
            dob.append(player.get("details").get("date_of_birth"))
            plays.append(player.get("details").get("plays"))
            weight.append(player.get("details").get("weight").split(" ")[0])
            height.append(player.get("details").get("height_meters"))
            prize_current_euros.append(player.get("details").get("prize_current_euros"))
            prize_total_euros.append(player.get("details").get("prize_current_euros"))

    # Sleep for 0.21 seconds so that the API limitations aren't reached
    time.sleep(0.21)


players = pd.DataFrame(
    columns=[
        "player_id",
        "player_first_name",
        "player_last_name",
        "country_code",
        "country",
        "dob",
        "plays",
        "weight",
        "height",
        "prize_current_euros",
        "prize_total_euros",
        "logo",
    ],
    data=np.column_stack(
        [
            player_id,
            player_first_name,
            player_last_name,
            country_code,
            country,
            dob,
            plays,
            weight,
            height,
            prize_current_euros,
            prize_total_euros,
            logo,
        ]
    ),
)

# TODO: fix pandas values that have splits on None objects like dob
"""
.split("(")[1],
                    "%d %b %Y)",
                )
                """
players.to_csv("test.csv", index=False)
