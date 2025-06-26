import sqlite3
import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from FitScore.fitScoreFunctions import *
from Clustering.matchTeamToCluster import match_team_to_cluster
from SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df
from SyntheticRosters.dataLoader import *

conn = sqlite3.connect('rosteriq.db')

def evaluate_transfer_fits(team_name, season_year, player_id_to_replace):    
    og_team_df = get_incoming_team_roster(conn, team_name, season_year)
    synthetic_team_df = og_team_df[og_team_df['player_id'] != player_id_to_replace]            
    player_rmvd = og_team_df[og_team_df['player_id'] == player_id_to_replace]
    player_rmvd_pos = player_rmvd['position'].values[0]

    # Get team stats and match them to a cluster
    synthethic_team_stats = aggregate_team_stats_from_players_df(synthetic_team_df)                      
    cluster_num, df = match_team_to_cluster(synthethic_team_stats)      

    transfer_data = get_transfers(conn, season_year, player_rmvd['position'].values[0])
    transfers_sim_scores = pd.DataFrame(columns=['player_name', 'sim_score'])

    for index, transfer in transfer_data.iterrows():                                                
        # get median values and standardized all players at the transfer playres position
        standardized_player_df, scaler = get_standardized_player_rate_stats(conn, season_year, cluster_num, player_rmvd_pos)
        median_vals_df = get_median_rate_stats_df(standardized_player_df, conn, season_year, cluster_num, player_rmvd_pos)
        # generate similiarity score
        player_sim_score = get_player_similarity_score(transfer.to_frame().T, 
                                            scaler, 
                                            median_vals_df.columns,
                                            median_vals_df)  
        player_name = transfer['player_name']
        transfers_sim_scores.loc[len(transfers_sim_scores)] = [player_name, player_sim_score]
    
    print(transfers_sim_scores.sort_values(by='sim_score', ascending=False))  
    print(transfers_sim_scores[transfers_sim_scores['player_name'] == "Aaron Cook"])  

# run
evaluate_transfer_fits("Gonzaga", 2021, 49449)








