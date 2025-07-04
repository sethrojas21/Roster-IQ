import sqlite3
import numpy as np
import pandas as pd
from standardization import *
from Clustering.matchTeamToCluster import match_team_to_cluster, match_team_to_cluster_weights
from SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df
from dataLoader import *

def calculate_fs_teamYear(conn, team_name, season_year, player_id_to_replace):
    synthetic_team_df, player_rmvd = get_incoming_synthetic_roster(conn, team_name, season_year, player_id_to_replace)    
    player_rmvd_pos = player_rmvd['position'].values[0]
    player_rmvd_name = player_rmvd['player_name'].values[0]
    # print(player_rmvd_name, player_id_to_replace, player_rmvd_pos, team_name, season_year)

    # Get team stats and match them to a cluster
    synthethic_team_stats = aggregate_team_stats_from_players_df(synthetic_team_df)                      
    # cluster_num, df = match_team_to_cluster(synthethic_team_stats, season_year)  
    closest_cluster_weights = match_team_to_cluster_weights(synthethic_team_stats, season_year)   
    for k, v in closest_cluster_weights.items():
        print(k, v)       

    query_snippet = """ps.efg_percent,
        ps.ast_percent,
        ps.oreb_percent,
        ps.dreb_percent,
        ps.tov_percent,
        ps.ft_percent,        
        ps.stl_percent,
        ps.blk_percent,
        (ps.threeA / ps.FGA) AS threeRate,
        ps.ftr,
        (ps.rimA / ps.FGA) AS rimRate,
        (ps.midA / ps.FGA) AS midRate
    """
    transfer_data = get_transfers(conn, season_year, player_rmvd['position'].values[0], query_snippet)

    # get scaler and median values
    # scaler, median_vals_df = get_nPercentile_info(query_snippet, conn, season_year, cluster_num, player_rmvd_pos, percentile=0.5)

    scaler, median_vals_df = get_nPercentile_scalar_and_vals(query_snippet, 
                                                             conn, 
                                                             season_year,                                                              
                                                             closest_cluster_weights,
                                                             player_rmvd_pos)
    
    scalar, role_dict = get_nPercentile_scalar_and_vals_roles(query_snippet, 
                                                             conn, 
                                                             season_year,                                                              
                                                             closest_cluster_weights,
                                                             player_rmvd_pos)

    transfers_sim_scores = pd.DataFrame(columns=['player_name', 'sim_score'])

    roles = ['bench', 'rotation', 'starter']        
    transfers_roles_sim_scores = pd.DataFrame(columns=['player_name', 'bench_sim_score', 'rotation_sim_score', 'starter_sim_score'])    
    for index, transfer in transfer_data.iterrows():                                                
        # generate similiarity score
        try:
            player_name = transfer['player_name']
            # For median of all roles
            # player_sim_score = get_player_similarity_score(transfer.to_frame().T, 
            #                                     scaler, 
            #                                     median_vals_df.columns,
            #                                     median_vals_df)  
            # transfers_sim_scores.loc[len(transfers_sim_scores)] = [player_name, player_sim_score]
            added_row = [player_name]
            for role in roles:
                med_vals_df = role_dict[role]                
                player_role_sim_score = get_player_similarity_score(transfer.to_frame().T, 
                                                scalar, 
                                                med_vals_df.columns,
                                                med_vals_df) 
                added_row.append(player_role_sim_score)
            transfers_roles_sim_scores.loc[len(transfers_roles_sim_scores)] = added_row            
        except:
            pass        
    
    # return transfers_sim_scores
    return transfers_roles_sim_scores 

# run
def test():
    conn = sqlite3.connect('rosteriq.db')
    df = calculate_fs_teamYear(conn, "Gonzaga", 2021, 49449)
    df_sorted = df.sort_values(by = 'rotation_sim_score', ascending=False)
    df_sorted = df_sorted.reset_index(drop=True)
    print(df_sorted)
    print(df_sorted[df_sorted['player_name'] == "Aaron Cook"])
    # df_sorted.to_csv('gonzagaFS.csv')
    # print(df_sorted)

test()