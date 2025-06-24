import sqlite3
import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from PredictBPM.classBPM import predict_bpm_tier_probs, role_modifier
from FitScore.fitScoreFunctions import *
from Clustering.matchTeamToCluster import match_team_to_cluster
from SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df


### Testing for Gonzaga 2021
"""
For zaga but write functions for all teams and years
    1. Select team and year (get their player stats from season before)
    2. Choose the transfer player we are going to remove
    3. Loop over all transfer players
        - Generate synthetic team using role modifier and last years stats
        - Find cluster team is part of
        - Generate fit score by finding median, standardized stats for players at transfer players
        position and years leading up to that
"""

conn = sqlite3.connect('rosteriq.db')

def get_incoming_team_roster_stats(conn, team_name, incoming_season_year):
    returners_query = """
    SELECT
        p.player_id,
        p.player_name,
        prev_ps.position,    
        prev_ps.FGA,
        prev_ps.FGM,
        prev_ps.FTA,
        prev_ps.threeM AS P3M,
        prev_ps.threeA AS P3A,                                
        prev_ps.adjoe,
        prev_ps.adrtg AS adjde,
        prev_ps.TOV,
        prev_ps.STL,                                
        prev_ps.OREB,
        prev_ps.DREB                               
    FROM Player_Seasons prev_ps
    JOIN (
        SELECT player_id
        FROM Player_Seasons                                             
        WHERE team_name = ? AND season_year = ?
    ) AS incomingRoster
      ON prev_ps.player_id = incomingRoster.player_id
    JOIN Players p
      ON prev_ps.player_id = p.player_id 
    JOIN Team_Seasons ts
        ON ts.team_name = prev_ps.team_name
       AND ts.season_year = prev_ps.season_year
    WHERE prev_ps.season_year = ?;
    """
    returners_df = pd.read_sql(returners_query, conn, params=(team_name, incoming_season_year, incoming_season_year - 1))    

    hs_query = f"""
    SELECT player_name, position, FGA, FGM, FTA, P3M, P3A, adjoe, adjde, TOV, OREB, DREB, bpm                     
    FROM HS_Rankings
    WHERE season_year = ? AND school_committed = ?
    """
    hs_df = pd.read_sql(hs_query, conn, params=(incoming_season_year - 1, team_name))

    return pd.concat([returners_df, hs_df])

def predict_roster_role_modifier(team_df : pd.DataFrame, player_bpm_features_df, prev_season_year, model):
    team_df['predicted_bpm'] = None
    team_df['role_modifier'] = None

    returner_ids = team_df['player_id'].dropna()
    
    for idx, player_id in enumerate(returner_ids):
        player_df = player_bpm_features_df[
        (player_bpm_features_df['player_id'] == player_id) &
        (player_bpm_features_df['prev_year'] == prev_season_year)
        ]
        player_name = team_df[team_df['player_id'] == player_id]['player_name'].values[0]
        print(player_name)        

        X = player_df.drop(columns=['bpm_to_predict', 'player_name', 'player_id', 'prev_year'])
        
        probs = predict_bpm_tier_probs(X)        

        role_modifier_val = role_modifier(probs)

        try:
            team_df.loc[idx, 'predicted_bpm'] = probs['tier_pred'].values[0]
            team_df.loc[idx, 'role_modifier'] = role_modifier_val.values[0]
        except:
            team_df.at[idx, 'predicted_bpm'] = 0.65
            team_df.at[idx, 'role_modifier']  = 0.65
    
    
    mask = team_df['player_id'].isna()
    bpm_vals = team_df.loc[mask, 'bpm']

    conds   = [ bpm_vals >  3,
                bpm_vals >= 0,
                bpm_vals <  0 ]
    tier_choices = [2,   1,   0]
    mod_choices  = [1.0, 0.9, 0.8]

    hs_predicted_bpm_val   = np.select(conds, tier_choices,   default=0)
    hs_role_modifier_val   = np.select(conds, mod_choices,    default=0)

    team_df.loc[mask, 'predicted_bpm'] = hs_predicted_bpm_val
    team_df.loc[mask, 'role_modifier'] = hs_role_modifier_val

### Testing
season_year = 2021
team_name = "Gonzaga"

team_df = get_incoming_team_roster_stats(conn, team_name, season_year)
rmv_name = "Aaron Cook"
rmv_plyr_df = team_df[team_df['player_name'] == rmv_name]
rmv_id = rmv_plyr_df['player_id'].values[0]
rmv_pos = rmv_plyr_df['position'].values[0]

all_player_df = pd.read_csv('Analysis/PredictBPM/bpm_features_all.csv')

model = XGBClassifier()
model.load_model('Analysis/PredictBPM/xgb_class_tier_bpm_model.json')

predict_roster_role_modifier(team_df, all_player_df, season_year - 1, model)

synthetic_roster_stats_df = aggregate_team_stats_from_players_df(team_df)

cluster_num, df = match_team_to_cluster(synthetic_roster_stats_df)

standardized_player_df, scaler = get_standardized_player_rate_stats(conn, season_year, cluster_num, rmv_pos)
median_vals_df = get_median_rate_stats_df(standardized_player_df, conn, season_year, cluster_num, rmv_pos)











