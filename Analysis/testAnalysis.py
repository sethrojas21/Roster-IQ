import sqlite3
import pandas as pd
from calcCompositeScore import composite_score
from calcFitScore import calculate_fs_teamYear
from calcVOCRP import calculate_VOCRP_teamYear

conn = sqlite3.connect('rosteriq.db')

avail_team_df = pd.read_csv('Analysis/availTransferTeams.csv')

skip = 0
see = 1
good_teams = ["Michigan", "Mississippi", "Gonzaga", "Tennessee"]
for idx, avail_team in avail_team_df.iterrows():
    team_name = avail_team['team_name']        
    # if idx < skip:
    #     continue
    # if team_name not in good_teams:
    #     continue 
    season_year = avail_team['season_year']
    player_id_to_replace = avail_team['player_id']  
    player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0] 
    print(player_name, team_name, season_year)
    try:
        # ts_df = calculate_fs_teamYear(conn, team_name, season_year, player_id_to_replace, sortByRole="rotation")
        # print(ts_df.head(10))
        # print(ts_df[ts_df['player_name'] == player_name])
        # vocbp_df = calculate_VOCRP_teamYear(conn, team_name, season_year, player_id_to_replace)
        # print(vocbp_df.head(10))
        # print(vocbp_df[vocbp_df['player_name'] == player_name])
        cs_df = composite_score(conn, team_name, season_year, player_id_to_replace)        

        print(cs_df.head(10))    
        print(cs_df[cs_df['player_name'] ==  player_name])
        print("-" * 10)
    except:
        print("FAILED TO FIND PLAYER")
    
    if idx > skip + see:
        exit()
    