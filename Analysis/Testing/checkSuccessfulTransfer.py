import sqlite3
import pandas as pd
import random
from Analysis.CalculateScores.calcCompositeScore import composite_score
from Analysis.EvaluateMetrics.successful_transfer import successful_transfer
from Analysis.config import Config
from Analysis.Helpers.queries import all_players_query, single_player_query

conn = sqlite3.connect('rosteriq.db')
positions = Config.POSITIONS

years_range = range(Config.START_YEAR, Config.END_YEAR_EXCLUDE)
plyr_df_dict = {}
for year in years_range:
    plyr_df_dict[year] = {}

TOP_PERCENT = Config.TOP_PERCENT
BOTTOM_PERCENT = Config.BOTTOM_PERCENT
ESS_THRESHOLD = Config.ESS_THRESHOLD

for year in years_range:
    team_df = pd.read_csv(f'Analysis/Clustering/15ClusterData/{year}/KClustering/labels.csv')
    print("Downloading", year)
    for pos in positions:
        plyr_team_df = pd.read_sql(all_players_query(pos), 
                                   conn,
                                   params = (year - 3, year))
        plyr_df = pd.read_csv(f'Analysis/Clustering/Players/{year}/KClustering/player_labels_{pos}.csv')
        merged_plyr_df = pd.merge(plyr_df, plyr_team_df, on=['player_name', 'season_year', 'team_name'])
        merged_df = pd.merge(merged_plyr_df, team_df, on=['team_name', 'season_year'])
        plyr_df_dict[year][pos] = merged_df

avail_team_df = pd.read_csv('/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Helpers/availTransferTeams.csv')

all_teams_barthag = pd.read_sql("SELECT team_name, season_year, barthag_rank FROM Team_Seasons GROUP BY team_name, season_year", conn)
top_teams_barthag = all_teams_barthag[all_teams_barthag['barthag_rank'] <= 90]
top_teams_df = pd.merge(avail_team_df, top_teams_barthag, on=['team_name', 'season_year'], how='left').dropna()
sampled_teams = avail_team_df.sample(n=1000, random_state=random.randint(1,100))

print("Starting to iterate over transfers")
correct = 0
succ_count = 0
unsucc_count = 0
total = 0
for idx, avail_team in avail_team_df.iterrows():
    team_name = avail_team['team_name']        

    season_year = avail_team['season_year']
    player_id_to_replace = avail_team['player_id']  
    player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0]
    position = conn.execute("SELECT position FROM Player_Seasons WHERE player_id = ? AND season_year = ?",
                            (player_id_to_replace, season_year)).fetchone()[0] 
    if team_name not in ["Arizona"]:
        continue
    print(player_name, position, team_name, season_year, player_id_to_replace)
    bmakr_plyr, cs_df = composite_score(conn, team_name, season_year, player_id_to_replace)

    plyr_cluster_id = bmakr_plyr.plyr_ids
    team_cluster_id = bmakr_plyr.team_ids
    
    plyr_query = single_player_query(position)

    plyr_stats = pd.read_sql(plyr_query, 
                             conn, 
                             params = (season_year, player_id_to_replace)).iloc[0]
    
    plyr_pos_stats = plyr_df_dict[season_year][position]
    try:
        score, is_succ, ess = successful_transfer(plyr_cluster_id,
                                             team_cluster_id,
                                             plyr_stats,
                                             plyr_pos_stats,
                                             bmakr_plyr.plyr_weights,
                                             bmakr_plyr.team_weights,
                                             debug=True)
        print(ess)
    except Exception as e:
        print(e)

    # ESS Cut-off
    if True and ess <= 20:
        print("ESS Sample Not MOve")
    
    try:
        rank = cs_df[cs_df['player_name'] == player_name].index[0]
    except:
        print("Skipping because was not here last season")
        print("-" * 10)
        continue

    length = len(cs_df)
    successPercentile = (rank <= length * TOP_PERCENT)
    unsuccessPercentile = (rank >= length * (1 - BOTTOM_PERCENT))
    successCond = successPercentile and is_succ
    unsuccessCond = unsuccessPercentile and not is_succ

    print(cs_df.head(5))
    print(cs_df.iloc[rank - 2 : rank + 2])
    
    print(f"""
ESS : {ess}
Pos: {position},  
Rank: {rank}, 
Num Players at Position: {length},
Length of B-Mark Sample: {bmakr_plyr.length},
Player Archetype(s): {bmakr_plyr.plyr_labels},
Player Weight(s) : {bmakr_plyr.plyr_weights}
Team Archetype(s): {bmakr_plyr.team_labels},
Team Weight(s) : {bmakr_plyr.team_weights}
Percentile Rank: { 1 - (rank / length)},
Projected Top%: {successPercentile}, 
Projected Bottom%: {unsuccessPercentile}, 
Considered Sucesss: {is_succ}, 
Success Score: {score}""")
    
    if successCond:
        total += 1 
        correct += 1
        print("Correct - Successful and Ranked High")
        succ_count += 1
    elif unsuccessCond:
        total += 1
        correct += 1
        print("Correct - Unsuccessful and Ranked Low")
        unsucc_count += 1
    elif not successPercentile and not is_succ:
        print("No worries")
    else:
        total += 1
        print("Incorrect")

    try:
        print(correct, total, correct / total)
    except ZeroDivisionError as e:
        print(e)
    print("-" * 20)

print(f"{correct} correct, {total} total, with a {correct / total}% rate")
print("Succ count", succ_count)
print("Unsucc count", unsucc_count)
total_succs = succ_count + unsucc_count
pSucc = succ_count / total_succs
pUnscc = unsucc_count / total_succs
print("% Succ", pSucc)
print("% Unsucc", pUnscc) 
print("Chance percentage", pSucc * TOP_PERCENT + pUnscc * BOTTOM_PERCENT)




