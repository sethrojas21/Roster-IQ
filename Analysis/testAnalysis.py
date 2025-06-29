import sqlite3
import pandas as pd
from calcCompositeScore import composite_score

conn = sqlite3.connect('rosteriq.db')

avail_team_df = pd.read_csv('Analysis/availTransferTeams.csv')

skip = 20
see = 10
for idx, avail_team in avail_team_df.iterrows():
    team_name = avail_team['team_name']
    if "Arizona" not in team_name:
        continue
    season_year = avail_team['season_year']
    player_id_to_replace = avail_team['player_id']  
    player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0] 
    print(player_name, team_name, season_year)
    try:
        cs_df = composite_score(conn, team_name, season_year, player_id_to_replace)        

        print(cs_df.head(10))    
        print(cs_df[cs_df['player_name'] ==  player_name])
        print("-" * 10)
    except:
        print("FAILED TO FIND PLAYER")