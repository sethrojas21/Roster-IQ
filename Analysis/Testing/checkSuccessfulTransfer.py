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
    if team_name not in ["Arizona"]:
        continue

    season_year = avail_team['season_year']
    player_id_to_replace = avail_team['player_id']  
    player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0]
    position = conn.execute("SELECT position FROM Player_Seasons WHERE player_id = ? AND season_year = ?",
                            (player_id_to_replace, season_year)).fetchone()[0] 
    print(player_name, position, team_name, season_year, player_id_to_replace)
    bmakr_plyr, cs_df = composite_score(conn, team_name, season_year, player_id_to_replace)
    if bmakr_plyr.length < 30:
        print("Skipping because not enough sample size")
        print("-" * 10)
        continue

    plyr_cluster_id = bmakr_plyr.plyr_ids[0]
    team_cluster_id = bmakr_plyr.team_ids[0]

    plyr_query = single_player_query(stat_queries_dict[position])

    plyr_stats = pd.read_sql(plyr_query, 
                             conn, 
                             params = (season_year, player_id_to_replace)).iloc[0]
    
    plyr_pos_stats = plyr_df_dict[season_year][position]
    success_percentage = successful_transfer(plyr_cluster_id,
                                             team_cluster_id,
                                             plyr_stats,
                                             plyr_pos_stats)
    print(success_percentage)




# is_success = successful_transfer(plyr_cluster_id,
#                                  team_cluster_id,
#                                  aaron_cook_stats,
#                                  merged_df)

# print("Percentage: ", is_success)



