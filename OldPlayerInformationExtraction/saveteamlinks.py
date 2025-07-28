import requests
import pickle
import csv
import pandas as pd
import json

teamnames = None

with open('OldPlayerInformationExtraction/apikeys.json', 'r') as file:
    data = json.load(file)['Google']
    api_key = data['api key']
    cse_id = data['cse key']


with open('/Users/sethrojas/documents/codeprojects/baresearch/TeamPlayerInformation/teamnames.csv', 'r') as file:
    teamnames = pd.read_csv(file)

# with open('/Users/sethrojas/documents/codeprojects/baresearch/TeamPlayerInformation/teamlinks.pkl', 'rb') as file:
#     team_links = pickle.load(file)

team_links = {}

for team in teamnames.iloc[270:, 0]:
    query = f"{team} Men's Basketball Team Roster Website"
    url = f"https://www.googleapis.com/customsearch/v1?q={query}&key={api_key}&cx={cse_id}"
    response = requests.get(url)
    data = response.json()

    team_link = data['items'][0]['link']
    team_name = team

    print(team_name, team_link)
    team_links[team_name] = team_link

