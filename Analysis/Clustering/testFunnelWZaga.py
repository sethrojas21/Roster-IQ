import sqlite3
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
# from Analysis.queries import statsFromPreviousSeason
from Analysis.PredictBPM.classBPM import predict_bpm_tier_probs, role_modifier
from Analysis.Clustering.matchTeamToCluster import match_team_to_cluster


# --- Aggregation function for team stats from player-level DataFrame ---
def aggregate_team_stats_from_players_df(df):
    """
    Given a DataFrame of player stats with columns:
    FGA, FGM, FTA, TOV, STL, OREB, DREB, P3M, P3A, adjoe, adjde, and any other stats,
    compute team-level aggregated metrics analogous to clusterTeams.r.
    Returns a dict with the aggregated stats.
    """
    # compute possessions per player
    df = df.copy()
    # apply role_modifier to each player's stats to forecast next-season impact
    stat_cols = ['adjoe','FGM','P3M','FGA','P3A','TOV','OREB','DREB','STL']
    df[stat_cols] = df[stat_cols].multiply(df['role_modifier'], axis=0)
    # compute inverse modifier, but never below 1
    inv_mod = (1 / df['role_modifier']).clip(lower=1.0)
    # apply to adjde
    df['adjde'] = df['adjde'] * inv_mod
    df['poss'] = df['FGA'] + 0.44 * df['FTA'] + df['TOV'] - df['OREB']
    total_poss = df['poss'].sum()    
    # weighted mean of adjoe and adjde by possessions
    team_adjoe = np.average(df['adjoe'], weights=df['poss']) if total_poss else np.nan
    team_adjde = np.average(df['adjde'], weights=df['poss']) if total_poss else np.nan
    # turnover-to-steal ratio
    team_stltov_ratio = df['TOV'].sum() / df['STL'].sum() if df['STL'].sum() else np.nan
    # offensive and defensive rebound rates per 100 possessions
    team_oreb_per100 = df['OREB'].sum() / total_poss * 100 if total_poss else np.nan
    team_dreb_per100 = df['DREB'].sum() / total_poss * 100 if total_poss else np.nan
    # effective field goal percentage
    team_eFG = (df['FGM'].sum() + 0.5 * df['P3M'].sum()) / df['FGA'].sum() if df['FGA'].sum() else np.nan
    # combined 3pt‐FGA metric from R code
    team_3pt_fga = 3 * (df['P3M'].sum() / df['FGA'].sum()) * (df['P3A'].sum() / df['FGA'].sum())    
    # drop NaNs from adjt and align weights
    valid = df['adjt'].notna()
    adjt_vals = df.loc[valid, 'adjt']
    adjt_weights = df.loc[valid, 'poss']
    team_adjt = np.average(adjt_vals, weights=adjt_weights) if len(adjt_vals) else np.nan
    return {
        'team_adjoe': team_adjoe,
        'team_adjde': team_adjde,
        'team_stltov_ratio': team_stltov_ratio,
        'team_oreb_per100': team_oreb_per100,
        'team_dreb_per100': team_dreb_per100,
        'team_eFG': team_eFG,
        # 'team_P3M' : int(df['P3M'].sum()),   
    }

conn = sqlite3.connect('rosteriq.db')

### Choose Gonzaga 2020

team = "Gonzaga"
# Retrieve all 2020 stats for players who were on Gonzaga's 2021 roster
gonzaga_prev_stats_df = pd.read_sql(f"""
    SELECT
                                p.player_name,
                                p.player_id,
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
                                prev_ps.DREB,
                                prev_ps.MIN,
                                ts.adjt                                    
    FROM Player_Seasons prev_ps
    JOIN (
        SELECT player_id
        FROM Player_Seasons                                             
        WHERE team_name = "{team}" AND season_year = 2021
    ) AS roster2021
      ON prev_ps.player_id = roster2021.player_id
    JOIN Players p
      ON prev_ps.player_id = p.player_id 
    JOIN Team_Seasons ts
        ON ts.team_name = prev_ps.team_name
       AND ts.season_year = prev_ps.season_year
    WHERE prev_ps.season_year = 2020;
""", conn)

# print(gonzaga_prev_stats_df)

ids = gonzaga_prev_stats_df['player_id']
names = gonzaga_prev_stats_df['player_name']
gonzaga_prev_stats_df['predicted_bpm'] = None
gonzaga_prev_stats_df['role_modifier'] = None
all_player_df = pd.read_csv('Analysis/PredictBPM/bpm_features_all.csv')

model = XGBClassifier()
model.load_model('Analysis/PredictBPM/xgb_class_tier_bpm_model.json')

for idx, player_id in enumerate(ids):
    # Retrieve the player's name by position
    player_name = names.iloc[idx]
    player_id = int(player_id)
    # Filter the bpm_features DataFrame for this player's previous year records
    player_df = all_player_df[
        (all_player_df['player_id'] == player_id) &
        (all_player_df['prev_year'] == 2020)
    ]
    X = player_df.drop(columns=['bpm_to_predict', 'player_name', 'player_id', 'prev_year'])
    print(f"Player: {player_name} (ID: {player_id})")
    # print(X)    
    # print("Predicted BPM: ")
    probs = predict_bpm_tier_probs(X)
    # print(probs)
    role_modifier_val = role_modifier(probs)
    # print(role_modifier_val)


    
    try:
        gonzaga_prev_stats_df.loc[idx, 'predicted_bpm'] = probs['tier_pred'].values[0]
        gonzaga_prev_stats_df.loc[idx, 'role_modifier'] = role_modifier_val.values[0]
    except:
        gonzaga_prev_stats_df.at[idx, 'predicted_bpm'] = 0.65
        gonzaga_prev_stats_df.at[idx, 'role_modifier']  = 0.65

# Load high-school rankings for 2020
hs_recruits_df = pd.read_sql(f"""
    SELECT player_name, position, FGA, FGM, P3M, P3A, MIN, bpm, adjoe, adjde, OREB, DREB, FTA, TOV                     
    FROM HS_Rankings
    WHERE season_year = 2020 AND school_committed = "{team}"
""", conn)


# Vectorized bucket assignment: 2 = >3, 1 = 0–3, 0 = <0
conds = [
    hs_recruits_df['bpm'] > 3,
    hs_recruits_df['bpm'] >= 0,
    hs_recruits_df['bpm'] < 0
]
choices = [2, 1, 0]
tier_weights = [1,0.9,0.8]
hs_recruits_df['predicted_bpm'] = np.select(conds, choices, default=0)
hs_recruits_df['role_modifier'] = np.select(conds, tier_weights, default=0)

# Append high-school recruits to the bottom of the Gonzaga previous stats
combined_df = pd.concat([gonzaga_prev_stats_df, hs_recruits_df], ignore_index=True)

# Print the combined DataFrame for verification
print("Combined Gonzaga Previous Stats with HS Recruits:")
print(combined_df[['player_name', 'adjoe', 'adjde', 'MIN', 'FGA', 'TOV', 'P3M', 'OREB', 'role_modifier', 'adjt']])
# --- Team-level aggregation using the above function ---
team_stats = aggregate_team_stats_from_players_df(combined_df)
print("Synthetic Team Aggregated Stats:")
for stat, value in team_stats.items():
    print(f"{stat}: {value}")

nearest, df = match_team_to_cluster(team_stats)
print(nearest)
print(df)
