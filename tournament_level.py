import requests

url = "https://livescore6.p.rapidapi.com/leagues/v2/list"

querystring = {"Category":"tennis"}

headers = {
    'x-rapidapi-host': "livescore6.p.rapidapi.com",
    'x-rapidapi-key': "811228ff06msh50e3dfe9f8a5ecfp1e975ejsn6b992dbfd77b"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)