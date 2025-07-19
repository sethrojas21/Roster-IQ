import sqlite3
import numpy as np
import pandas as pd
from standardization import *
from Clustering.matchTeamToCluster import match_team_to_cluster_weights
from Clustering.matchPlayerToCluster import match_player_to_cluster_weights, get_player_stats
from SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df
from dataLoader import *
from calcMetricHelpers import fs_query_snippet, calc_score_data_helper

def calculate_fs_teamYear(conn, team_name, season_year, player_id_to_replace, sortByRole = True):
    scalar, med_vals_df, transfer_data = calc_score_data_helper(fs_query_snippet, 
                                                              conn, team_name, 
                                                              season_year, 
                                                              player_id_to_replace)
    
    transfers_roles_sim_scores = pd.DataFrame(columns=['player_name', 'archs_sim_score'])    
    
    for index, transfer in transfer_data.iterrows():                                                
        # generate similiarity score
        try:
            player_name = transfer['player_name']
            # For median of all roles
            added_row = [player_name]           
            player_role_sim_score = get_player_similarity_score(transfer.to_frame().T, 
                                            scalar, 
                                            med_vals_df.columns,
                                            med_vals_df) 
            added_row.append(player_role_sim_score)
            transfers_roles_sim_scores.loc[len(transfers_roles_sim_scores)] = added_row            
        except Exception as e:
            print(e)
        
    if sortByRole:  
        transfers_roles_sim_scores = transfers_roles_sim_scores.sort_values(by=('archs_sim_score'), ascending=False)
        transfers_roles_sim_scores = transfers_roles_sim_scores.reset_index(drop = True)

    return transfers_roles_sim_scores 

# run
def test():
    conn = sqlite3.connect('rosteriq.db')
    df = calculate_fs_teamYear(conn, "Gonzaga", 2021, 49449)
    df_sorted = df
    df_sorted = df_sorted.reset_index(drop=True)
    print(df_sorted.head(20))
    print(df_sorted[df_sorted['player_name'] == "Aaron Cook"])
    # df_sorted.to_csv('gonzagaFS.csv')
    # print(df_sorted)

# test()