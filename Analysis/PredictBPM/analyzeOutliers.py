import pickle
import sqlite3
import pandas as pd
from queries import understandFeaturesOfOutliersQuery

with open('Analysis/outliersDF.pkl', 'rb') as f:
    outliersDF = pickle.load(f)

conn = sqlite3.connect('rosteriq.db')

df = pd.read_sql_query(understandFeaturesOfOutliersQuery, conn)

gamesPlayedCount = 0
count = 0
print(len(df))
for index, player in outliersDF.iterrows():
    if count > 50: break
    player_id = player['player_id']
    prev_season_year = player['prev_year']

    prev_year_player_data = df[(df['player_id'] == player_id) & (df['season_year'] == prev_season_year)]
    next_year_player_data = df[(df['player_id'] == player_id) & (df['season_year'] == (prev_season_year + 1))]

    if (next_year_player_data['games_played'].values[0] < 5):
        gamesPlayedCount+= 1

    print(prev_year_player_data)
    print(next_year_player_data)
    print("Actual: ", player['Actual_BPM'])
    print("Predicted:", player['Predicted_BPM'])
    
    count+= 1

print(gamesPlayedCount)
    


