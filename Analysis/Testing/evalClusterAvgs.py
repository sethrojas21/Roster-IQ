import pandas as pd
from Analysis.Config.config import Config
import sqlite3
import itertools
import numpy as np


conn = sqlite3.connect('rosteriq.db')
 
cluster_df = pd.DataFrame(columns=['season_year', 'pos', 'team_clu_id', 'player_clu_id', 'length', 'median', 'std'])
for year in range(2021, 2025):

    team_df = pd.read_csv(f'Analysis/Clustering/15ClusterData/{year}/KClustering/labels.csv')
    for pos in Config.POSITIONS:
        pos_df = pd.read_csv(f'Analysis/Clustering/Players/{year}/KClustering/player_labels_{pos}.csv')

        plyr_pos_stats_df = pd.read_sql("""SELECT
                                        player_id,
                                        season_year,
                                        bpm
                                        FROM Player_Seasons
                                        WHERE season_year >= ? AND season_year < ? """,
                                        conn,
                                        params=(year - Config.LOOKBACK_YEAR , year))
        
        # merge to get player's bpm
        plyr_merged_df = pd.merge(pos_df, plyr_pos_stats_df, how='left', on=['player_id', 'season_year'])

        # merge to add on team cluster id
        merged_df = pd.merge(plyr_merged_df, team_df, how='left', on=['team_name', 'season_year'])

        # get the ids
        team_cluster_ids = sorted(merged_df['team_cluster'].unique())
        player_cluster_ids = sorted(merged_df['Cluster'].unique())

        
        # double for to get bpm on each cluster
        for team_id in team_cluster_ids:
            for player_id in player_cluster_ids:
                filtered_df = merged_df[(merged_df['Cluster'] == player_id) & (merged_df['team_cluster'] == team_id)]
                bpm = filtered_df['bpm']
                length = len(bpm)
                median = bpm.median()  # bpm of player and team cluster
                std = bpm.std()

                cluster_df.loc[len(cluster_df)] = [year, pos, team_id, player_id, length, median, std]
        
        # print(pos)
        # for player_id in player_cluster_ids:
        #     plyr_clu = cluster_df[(cluster_df['pos'] == pos) & (cluster_df['team_clu_id'] == 2) & (cluster_df['player_clu_id'] == player_id)].iloc[0]
        #     print(f"Cluster {player_id}", "Lenght", plyr_clu['length'], "Median", plyr_clu['median'],"STD", plyr_clu['std'])

cluster_df.to_csv('Analysis/Testing/CSVs/cluster_info.csv', index=False)

    


    

    
            

    
        

