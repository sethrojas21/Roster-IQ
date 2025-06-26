from PredictBPM.classBPM import predict_bpm_tier_probs, role_modifier
import pandas as pd

def predict_roster_role_modifier(team_df : pd.DataFrame, player_bpm_features_df, prev_season_year, model):
    team_df['predicted_bpm'] = None
    team_df['role_modifier'] = None

    returner_ids = team_df['player_id'].dropna()
    
    for idx, player_id in returner_ids.items():
        player_df = player_bpm_features_df[
        (player_bpm_features_df['player_id'] == player_id) &
        (player_bpm_features_df['prev_year'] == prev_season_year)
        ]
        player_name = team_df[team_df['player_id'] == player_id]['player_name'].values[0]                
        
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