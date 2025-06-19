import sqlite3
import pandas as pd
from xgboost import XGBClassifier
# from Analysis.queries import statsFromPreviousSeason
from Analysis.PredictBPM.classBPM import predict_bpm_tier_probs, role_modifier

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
                                prev_ps.OREB,
                                prev_ps.DREB                                    
    FROM Player_Seasons prev_ps
    JOIN (
        SELECT player_id
        FROM Player_Seasons
        WHERE team_name = "{team}" AND season_year = 2021
    ) AS roster2021
      ON prev_ps.player_id = roster2021.player_id
    JOIN Players p
      ON prev_ps.player_id = p.player_id
    WHERE prev_ps.season_year = 2020;
""", conn)

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
    print(X)    
    print("Predicted BPM: ")
    probs = predict_bpm_tier_probs(X)
    print(probs)
    role_modifier_val = role_modifier(probs)
    print(role_modifier_val)
    # gonzaga_prev_stats_df[gonzaga_prev_stats_df['player_id'] == player_id]['predicted_bpm'] = probs['tier_pred']
    # gonzaga_prev_stats_df[gonzaga_prev_stats_df['player_id'] == player_id]['role_modifier'] = role_modifier_val


# Load high-school rankings for 2020
hs_recruits_df = pd.read_sql(f"""
    SELECT player_name, position, FGA, FGM, P3M, P3A, bpm, adjoe, adjde, OREB, DREB, FTA                     
    FROM HS_Rankings
    WHERE season_year = 2020 AND school_committed = "{team}"
""", conn)

# Append high-school recruits to the bottom of the Gonzaga previous stats
combined_df = pd.concat([gonzaga_prev_stats_df, hs_recruits_df], ignore_index=True)

# Print the combined DataFrame for verification
# print("Combined Gonzaga Previous Stats with HS Recruits:")
# print(combined_df)

# combined_df.to_csv('gonzaga_2021.csv')
