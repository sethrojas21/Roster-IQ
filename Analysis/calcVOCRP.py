import sqlite3
import numpy as np
import pandas as pd
from Clustering.matchTeamToCluster import match_team_to_cluster, match_team_to_cluster_weights
from SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df
from standardization import *
from dataLoader import *

def player_difference(player_stats_df,
                      scaler,
                      columns,
                      nPercentile_vals):
    player_vec = scale_player_stats(player_stats_df, scaler, columns)
    # nPercentile_vec = nPercentile_vals[columns].values.reshape(1, -1)
    difference = np.subtract(player_vec, nPercentile_vals)
    return difference

def avg_zScore_deviation(diff_vec):    
    return diff_vec.iloc[0].sum() / len(diff_vec.iloc[0])


def calculate_VOCRP_teamYear(conn, team_name, incoming_season_year, player_id_to_replace, sort = True):
    synthetic_team_df, player_rmvd = get_incoming_synthetic_roster(conn, team_name, incoming_season_year, player_id_to_replace)    
    player_rmvd_pos = player_rmvd['position'].values[0]

    # Get team stats and match them to a cluster
    synthethic_team_stats = aggregate_team_stats_from_players_df(synthetic_team_df)                      
    cluster_num, df = match_team_to_cluster(synthethic_team_stats, incoming_season_year)  
    closest_cluster_weights = match_team_to_cluster_weights(synthethic_team_stats, incoming_season_year) 
    for k, v in closest_cluster_weights.items():
        print(k, v)    
    query_snippet = """
        (ps.AST / ps.POSS) * 100 AS ast100,
        (ps.OREB / ps.POSS) * 100 AS oreb100,
        (ps.DREB / ps.POSS) * 100 AS dreb100,
        (CAST(ps.STL AS REAL) * 100 / ps.POSS) AS stl100,
        (CAST(ps.BLK AS REAL) * 100 / ps.POSS) AS blk100,        
        ps.ts_percent,        
        ps.ast_tov_r
    """

    transfer_data = get_transfers(conn, incoming_season_year, player_rmvd['position'].values[0], query_snippet)

    scaler, nPercentile_vals_df = get_nPercentile_info(query_snippet, 
                                                       conn, 
                                                       incoming_season_year, 
                                                       cluster_num, player_rmvd_pos, 
                                                       percentile=0.75)
    
        # get scaler and median values
    scalar, role_dict = get_nPercentile_scalar_and_vals_roles(query_snippet, 
                                                             conn, 
                                                             incoming_season_year,                                                              
                                                             closest_cluster_weights,
                                                             player_rmvd_pos)


    roles = {'bench': 0.1, 'rotation' : 0.3, 'starter' : 0.6}     
    vocbp_scores = pd.DataFrame(columns=['player_name', 'vocbp']) 
    
    
    # vocrp_scores = pd.DataFrame(columns=['player_name', 'vocrp'])
    for index, transfer in transfer_data.iterrows():

        # diff_vec = player_difference(transfer.to_frame().T, 
        #                                         scaler, 
        #                                         nPercentile_vals_df.columns,
        #                                         nPercentile_vals_df)
        # vocrp = avg_zScore_deviation(diff_vec)             
        # vocrp_scores.loc[len(vocrp_scores)] = [transfer['player_name'], vocrp]
        try:
            player_name = transfer['player_name']
            # For median of all roles            
            weighted_sum_median = 0
            for role, role_weight in roles.items():
                med_vals_df = role_dict[role]                
                player_role_vocbp = player_difference(transfer.to_frame().T, 
                                                scalar, 
                                                med_vals_df.columns,
                                                med_vals_df) 
                weighted_sum_median += role_weight * player_role_vocbp

            vocbp = avg_zScore_deviation(weighted_sum_median)
            vocbp_scores.loc[len(vocbp_scores)] = [player_name, vocbp]            
        except:
            pass  

    if sort:
        vocbp_scores = vocbp_scores.sort_values(by='vocbp', ascending=False)
        vocbp_scores = vocbp_scores.reset_index(drop=True) 
    
    return vocbp_scores


def testing():
    conn = sqlite3.connect('rosteriq.db')
    df = calculate_VOCRP_teamYear(conn, "Gonzaga", 2021, 49449)
    df_sorted = df.sort_values(by = 'vocbp', ascending=False)
    df_sorted = df_sorted.reset_index(drop=True)
    print(df_sorted)
    print(df_sorted[df_sorted['player_name'] == "Aaron Cook"])
    # df_sorted.to_csv('gonzagaFS.csv')
    # print(df_sorted)

# testing()