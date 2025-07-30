import sqlite3
import pandas as pd

columns = ['player_name', 'season_year', 'Cluster', 'team_cluster']
positions = ["G", "F", "C"]
df = pd.DataFrame(columns=columns)


conn = sqlite3.connect('rosteriq.db')
for year in range(2021, 2025):
    year_df = pd.DataFrame(columns=columns)

    poses_df = pd.DataFrame(columns=columns[:-1]) # don't include team clsuter
    for pos in positions:
        pos_df = pd.read_csv(f'Analysis/Clustering/Players/{year}/KClustering/player_labels_{pos}.csv')
        poses_df = pd.concat([poses_df, pos_df], ignore_index=True)
    
    player_team_df = pd.read_sql("""
                                SELECT
                                 p.player_name,
                                 ps.team_name,
                                 ps.season_year
                                FROM Player_Seasons ps
                                JOIN Players p ON ps.player_id = p.player_id
                                WHERE ps.season_year >= ? AND ps.season_year < ?""", conn, params=(year - 3, year))
    
    team_df = pd.read_csv(f'Analysis/Clustering/15ClusterData/{year}/KClustering/labels.csv')

    merged_plyr_df = pd.merge(poses_df, player_team_df, on=['player_name', 'season_year'], how='right')
    print(poses_df)
    # print(merged_plyr_df)

