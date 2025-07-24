import sqlite3
import pandas as pd
from Analysis.CalculateScores.calcCompositeScore import composite_score
from Analysis.EvaluateMetrics.successful_transfer import is_successful_transfer
import random

conn = sqlite3.connect('rosteriq.db')

avail_team_df = pd.read_csv('/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Helpers/availTransferTeams.csv')

skip = 0
see = 40

all_teams_barthag = pd.read_sql("SELECT team_name, season_year, barthag_rank FROM Team_Seasons GROUP BY team_name, season_year", conn)
top_teams_barthag = all_teams_barthag[all_teams_barthag['barthag_rank'] <= 90]

is_successful_dict = {}

for year in range(2021, 2025):
    is_successful_dict[year] = is_successful_transfer(year - 1, year, conn)
    
corrCount = 0
totalCount = 1
num_teams = len(avail_team_df)

topPercent = 0.3
bottomPercent = 0.4

top_teams_df = pd.merge(avail_team_df, top_teams_barthag, on=['team_name', 'season_year'], how='left').dropna()
sampled_teams = avail_team_df.sample(n=see, random_state=random.randint(1,100))
# sampled_teams = avail_team_df

for idx, avail_team in avail_team_df.iterrows():
    team_name = avail_team['team_name']        
    # if team_name not in ["North Carolina", "Kentucky", "Michigan", "UCLA"]:
    #     continue

    season_year = avail_team['season_year']
    player_id_to_replace = avail_team['player_id']  
    player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0] 
    print(player_name, team_name, season_year, player_id_to_replace)

    
    success_df = is_successful_dict[season_year]
    success_row_df = success_df[success_df['player_name'] == player_name]
    success_row = success_row_df.iloc[0]

    is_success = success_row['successful']
    success_score = success_row['success_score']

    position = success_row['position']        
    role = success_row['new_role']

    bmakr_plyr, cs_df = composite_score(conn, team_name, season_year, player_id_to_replace)                
    if bmakr_plyr.length < 30:
        print("Skipping because not enough sample size")
        continue        
    # print(success_row_df)    
    print(cs_df[cs_df['player_name'] == player_name])           
    try:
        rank = cs_df[cs_df['player_name'] == player_name].index[0]
    except:
        print("Skipping because was not here last season")
        continue

    print(cs_df.head(15))

    length = len(cs_df)
    successPercentile = (rank <= length * topPercent)
    unsuccessPercentile = (rank >= length * (1 - bottomPercent))
    successCond = successPercentile and is_success
    unsuccessCond = unsuccessPercentile and not is_success        
    

    print(f"""
Pos: {position}, 
Role: {role}, 
Rank: {rank}, 
Length of B-Mark Sample: {bmakr_plyr.length},
Player Archetype(s): {bmakr_plyr.plyr_labels},
Player Weight(s) : {bmakr_plyr.plyr_weights}
Team Archetype(s): {bmakr_plyr.team_labels},
Team Weight(s) : {bmakr_plyr.team_weights}
Num Players at Position: {length},
Percentile Rank: { 1 - (rank / length)},
Projected Top%: {successPercentile}, 
Projected Bottom%: {unsuccessPercentile}, 
Considered Sucesss: {is_success}, 
Success Score: {success_score}""")
    
    if successCond or unsuccessCond:
        corrCount += 1
        print("Correct", corrCount, totalCount)
    
    if not successCond and not unsuccessCond and not is_success:
        totalCount -= 1

    totalCount += 1
    print("-" * 10)
    
    
    # if idx > skip + see:
    #     print(corrCount / (totalCount - 1))
    #     exit()
totalCount -= 1    
print(corrCount / totalCount, totalCount)
