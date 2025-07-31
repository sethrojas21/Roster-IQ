import pandas as pd
from Analysis.Config.config import Config
import sqlite3
import itertools
import numpy as np


conn = sqlite3.connect('rosteriq.db')

bpm_dict = {}  # store dict of bpms 
bpm_df = pd.DataFrame(columns=['season_year', 'pos', 'team_clu_id', 'player_clu_id', 'bpm'])
for year in range(2021, 2022):

    team_df = pd.read_csv('Analysis/Clustering/15ClusterData/2021/KClustering/labels.csv')
    bpm_dict[year] = {}
    for pos in Config.POSITIONS:
        pos_df = pd.read_csv(f'Analysis/Clustering/Players/2021/KClustering/player_labels_{pos}.csv')

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

        bpm_dict[year][pos] = {}
        # double for to get bpm on each cluster
        for team_id in team_cluster_ids:
            bpm_dict[year][pos][team_id] = {}
            for player_id in player_cluster_ids:
                filtered_df = merged_df[(merged_df['Cluster'] == player_id) & (merged_df['team_cluster'] == team_id)]
                bpm = filtered_df['bpm'].median()  # bpm of player and team cluster
                bpm_dict[year][pos][team_id][player_id] = bpm
                bpm_df.loc[len(bpm_df)] = [year, pos, team_id, player_id, bpm]
        
        print(pos)
        print("Length of team cluster ids", len(team_cluster_ids))
        print("Length of player cluster ids", len(player_cluster_ids), player_cluster_ids)

        plyr_cluster_diff = (
            bpm_df[bpm_df['pos'] == pos]
            .groupby(['season_year','player_clu_id'], as_index=False)
            .agg(
                median_bpm=('bpm', 'median'),
                std_dev=('bpm', 'std'),
                range=('bpm', lambda x: x.max() - x.min()),
            )
        )
        print(plyr_cluster_diff)

    # Calculate within-team BPM differences without FutureWarning
    within_team_diff = (
        bpm_df
        .groupby(['season_year', 'pos', 'team_clu_id'], as_index=False)
        .agg(
            mean_bpm=('bpm', 'mean'),
            std_dev=('bpm', 'std'),
            range=('bpm', lambda x: x.max() - x.min()),
            pct_range=('bpm', lambda x: ((x.max() - x.min()) / abs(x.mean()) * 100)
                                        if x.mean() != 0 else 0)
        )
    )

    print("Within-team BPM differences:")
    print(within_team_diff)

    


    

    
            

    
        

