import sqlite3
import pandas as pd
from Analysis.CalculateScores.calcCompositeScore import composite_score
from Analysis.EvaluateMetrics.successful_transfer import successful_transfer

conn = sqlite3.connect('rosteriq.db')

positions = ["G", "F", "C"]
gen_player_stats_query = """
ps.ts_percent,
ps.porpag,
ps.dporpag,
"""

g_query = """
ps.ast_percent,
ps.dreb_percent,
ps.stl_percent
"""

f_query = """
ps.ast_percent,
ps.dreb_percent,
ps.oreb_percent,
ps.blk_percent
"""

c_query = """
ps.dreb_percent,
ps.oreb_percent,
ps.blk_percent
"""

stat_queries_dict = {
    "G" : g_query,
    "F" : f_query,
    "C" : c_query
}

stats_query = lambda pos_query : f"""
SELECT 
    p.player_name,
    ps.position,
    ps.season_year, 
    ps.team_name,
    {gen_player_stats_query}
    {pos_query}
FROM Player_Seasons ps
JOIN Players p ON ps.player_id = p.player_id
"""

all_players_query = lambda pos_query: f"""
{stats_query(pos_query)}
WHERE ps.season_year >= ? AND ps.season_year < ?"""

single_player_query = lambda pos_query : f"""
{stats_query(pos_query)}
WHERE ps.season_year = ? AND ps.player_id = ?
"""

years_range = range(2021, 2025)
plyr_df_dict = {}
for year in years_range:
    plyr_df_dict[year] = {}


TOP_PERCENT = 0.3
BOTTOM_PERCENT = 0.4
SAMPLE_SIZE_THRESHOLD = 30
for year in years_range:
    team_df = pd.read_csv(f'Analysis/Clustering/15ClusterData/{year}/KClustering/labels.csv')
    print("Downloading", year)
    for pos in positions:
        pos_query = stat_queries_dict[pos]
        plyr_team_df = pd.read_sql(all_players_query(pos_query), 
                                   conn,
                                   params = (year - 3, year))
        plyr_df = pd.read_csv(f'Analysis/Clustering/Players/{year}/KClustering/player_labels_{pos}.csv')
        merged_plyr_df = pd.merge(plyr_df, plyr_team_df, on=['player_name', 'season_year'])
        merged_df = pd.merge(merged_plyr_df, team_df, on=['team_name', 'season_year'])
        plyr_df_dict[year][pos] = merged_df

avail_team_df = pd.read_csv('/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Helpers/availTransferTeams.csv')

print("Starting to iterate over transfers")
for idx, avail_team in avail_team_df.iterrows():
    team_name = avail_team['team_name']        

    season_year = avail_team['season_year']
    player_id_to_replace = avail_team['player_id']  
    player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0]
    position = conn.execute("SELECT position FROM Player_Seasons WHERE player_id = ? AND season_year = ?",
                            (player_id_to_replace, season_year)).fetchone()[0] 
    if team_name not in ["Gonzaga"]:
        continue
    print(player_name, position, team_name, season_year, player_id_to_replace)
    bmakr_plyr, cs_df = composite_score(conn, team_name, season_year, player_id_to_replace)
    if bmakr_plyr.length < SAMPLE_SIZE_THRESHOLD:
        print("Skipping because not enough sample size")
        print("-" * 10)
        continue

    plyr_cluster_id = bmakr_plyr.plyr_ids
    team_cluster_id = bmakr_plyr.team_ids

    plyr_query = single_player_query(stat_queries_dict[position])

    plyr_stats = pd.read_sql(plyr_query, 
                             conn, 
                             params = (season_year, player_id_to_replace)).iloc[0]
    
    plyr_pos_stats = plyr_df_dict[season_year][position]
    score, is_succ = successful_transfer(plyr_cluster_id,
                                             team_cluster_id,
                                             plyr_stats,
                                             plyr_pos_stats,
                                             bmakr_plyr.plyr_weights,
                                             bmakr_plyr.team_weights)
    
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
    
    if successCond or unsuccessCond:
        print("*" * 5,"Correct", "*" * 5)
    print("-" * 20)




# is_success = successful_transfer(plyr_cluster_id,
#                                  team_cluster_id,
#                                  aaron_cook_stats,
#                                  merged_df)

# print("Percentage: ", is_success)



