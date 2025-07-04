import sqlite3
import numpy as np
import pandas as pd
from standardization import scale_player_stats
from calcMetricHelpers import vocbp_query_snippet, calc_score_data_helper

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

def calculate_VOCRP_teamYear(conn, team_name, incoming_season_year, player_id_to_replace, sort=True): 
    scalar, role_dict, transfer_data = calc_score_data_helper(vocbp_query_snippet,
                                                              conn,
                                                              team_name,
                                                              incoming_season_year,
                                                              player_id_to_replace,)

    roles = {'bench': 0.1, 'rotation' : 0.3, 'starter' : 0.6}     
    vocbp_scores = pd.DataFrame(columns=['player_name', 'vocbp']) 
    
    for index, transfer in transfer_data.iterrows():
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