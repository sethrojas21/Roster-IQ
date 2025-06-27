import sqlite3
import numpy as np
import pandas as pd
from Clustering.matchTeamToCluster import match_team_to_cluster
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

def calc_vocrp(diff_vec):    
    return diff_vec.iloc[0].sum() / len(diff_vec.iloc[0])


def calculate_VOCRP_teamYear(conn, team_name, incoming_season_year, player_id_to_replace):
    synthetic_team_df, player_rmvd = get_incoming_synthetic_roster(conn, team_name, incoming_season_year, player_id_to_replace)    
    player_rmvd_pos = player_rmvd['position'].values[0]

    # Get team stats and match them to a cluster
    synthethic_team_stats = aggregate_team_stats_from_players_df(synthetic_team_df)                      
    cluster_num, df = match_team_to_cluster(synthethic_team_stats, incoming_season_year)   

    query_snippet = """(ps.PTS / ps.POSS) * 100 AS pts100,
        (ps.AST / ps.POSS) * 100 AS ast100,
        (ps.OREB / ps.POSS) * 100 AS oreb100,
        (ps.DREB / ps.POSS) * 100 AS dreb100,
        (CAST(ps.STL AS REAL) * 100 / ps.POSS) AS stl100,
        (CAST(ps.BLK AS REAL) * 100 / ps.POSS) AS blk100,
        ps.efg_percent,
        ps.ts_percent,
        ps.ast_tov_r
    """

    transfer_data = get_transfers(conn, incoming_season_year, player_rmvd['position'].values[0], query_snippet)

    scaler, nPercentile_vals_df = get_nPercentile_info(query_snippet, 
                                                       conn, 
                                                       incoming_season_year, 
                                                       cluster_num, player_rmvd_pos, 
                                                       percentile=0.75)
    
    
    vocrp_scores = pd.DataFrame(columns=['player_name', 'vocrp'])
    for index, transfer in transfer_data.iterrows():

        diff_vec = player_difference(transfer.to_frame().T, 
                                                scaler, 
                                                nPercentile_vals_df.columns,
                                                nPercentile_vals_df)
        vocrp = calc_vocrp(diff_vec)             
        vocrp_scores.loc[len(vocrp_scores)] = [transfer['player_name'], vocrp]
    
    return vocrp_scores

conn = sqlite3.connect('rosteriq.db')
df = calculate_VOCRP_teamYear(conn, "Gonzaga", 2021, 49449)

sorted_df = df.sort_values(by='vocrp', ascending=False)

print(sorted_df)