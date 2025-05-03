from queries import gptTransferQuery, statsFromPreviousSeason, playerRostersIncomingSeason
import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import numpy as np
conn = sqlite3.connect('rosteriq.db')

feature_rows = []

for year in range(2018, 2024):
    transferPlayersDF = pd.read_sql_query(gptTransferQuery, conn, params=(year, year+1))
    statsFromPreviousSeasonDF = pd.read_sql_query(statsFromPreviousSeason, conn, params=(year,))
    playerRostersIncomingSeasonDF = pd.read_sql_query(playerRostersIncomingSeason, conn, params=(year+1,))

    statsFromTransferPlayersPrevSeasonDF = pd.merge(
        transferPlayersDF,
        statsFromPreviousSeasonDF,
        on='player_id',
    )

    statsFromPlayersWhoHadAPreviousSeasonDF = pd.merge(
        statsFromPreviousSeasonDF,
        playerRostersIncomingSeasonDF,
        on=['player_id']
    )


    # label_encoder = LabelEncoder()
    # statsFromTransferPlayersPrevSeasonDF['position_encoded'] = label_encoder.fit_transform(
    #     statsFromTransferPlayersPrevSeasonDF['position_x']
    # )
    # statsFromTransferPlayersPrevSeasonDF['team_encoded'] = label_encoder.fit_transform(
    #     statsFromTransferPlayersPrevSeasonDF['old_team']
    # )
    
    for index, player in statsFromTransferPlayersPrevSeasonDF.iterrows():
        player_id = player['player_id']
        new_team = player['new_team']
        bpm_to_predict = statsFromPlayersWhoHadAPreviousSeasonDF[
            statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] == player_id
            ]['bpm_to_predict'].values[0]
        next_team_barthag_rank = statsFromPlayersWhoHadAPreviousSeasonDF[
            statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] == player_id
            ]['next_team_barthag_rank'].values[0]
        next_year_usg_rate = statsFromPlayersWhoHadAPreviousSeasonDF[
            statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] == player_id
            ]['next_year_usg_rate'].values[0]
        
        
        if pd.isna(bpm_to_predict):
            print(f"Skipping player {player['player_name_x']} due to missing bpm_to_predict.")
            continue
                
        # Get incoming teammates (who had a previous season)
        teammates = statsFromPlayersWhoHadAPreviousSeasonDF[
            (statsFromPlayersWhoHadAPreviousSeasonDF['next_team_name'] == new_team) &
            (statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] != player_id)
        ]
        
        if teammates.empty and len(teammates) < 8:
            print("Teammates no")
            continue
        
        try:
            incoming_team_barthag_from_last_season = teammates[teammates['prev_team_name'] == new_team].iloc[0, :]['prev_team_barthag_rank']
            incoming_team_adjoe_from_last_season = teammates[teammates['prev_team_name'] == new_team].iloc[0, :]['team_adj_off']
            incoming_team_adjde_from_last_season = teammates[teammates['prev_team_name'] == new_team].iloc[0, :]['team_adj_def']
        except:
            continue
        

        # Weighted averages for teammates        
        total_minutes = teammates['total_player_minutes'].sum()
        # Weighted and unweighted averages
        weighted_bpm = (teammates['bpm'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_bpm = teammates['bpm'].mean()
        avg_teammate_bpm = 0.7 * weighted_bpm + 0.3 * unweighted_bpm
        weighted_usg = (teammates['usg_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_usg = teammates['usg_percent'].mean()
        avg_teammate_usg = 0.7 * weighted_usg + 0.3 * unweighted_usg
        weighted_efg = (teammates['efg_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_efg = teammates['efg_percent'].mean()
        avg_teammate_efg = 0.7 * weighted_efg + 0.3 * unweighted_efg
        weighted_ast = (teammates['ast_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_ast = teammates['ast_percent'].mean()
        avg_teammate_ast = 0.7 * weighted_ast + 0.3 * unweighted_ast
        avg_teammate_2ptPercent = (teammates['two_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        avg_teammate_3ptPercent = (teammates['three_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        avg_teammate_adjt = (teammates['adjt'] * teammates['total_player_minutes']).sum() / total_minutes

        # Relative (role fit) features
        rel_bpm = player['bpm'] - avg_teammate_bpm
        rel_usg = player['usg_percent'] - avg_teammate_usg
        rel_efg = player['efg_percent'] - avg_teammate_efg
        rel_ast = player['ast_percent'] - avg_teammate_ast
        rel_adjt = player['adjt'] - avg_teammate_adjt
        rel_two_percent = player['two_percent'] - avg_teammate_2ptPercent
        rel_three_percent = player['three_percent'] - avg_teammate_3ptPercent
        

        diffInBarthagRank = next_team_barthag_rank - player['prev_team_barthag_rank']
        
        # Build feature row
        row = {
            'player_id': player['player_id'],   
            # 'player_position' : player['position_encoded'],
            'prev_year': year,
            'prev_team_cluster' : player['prev_team_cluster'],
            # 'team_name': player['team_encoded'],              
            'player_bpm_prev': player['bpm'],
            # 'percentOfTeamMinutes' : player['total_player_minutes'] / player['total_team_minutes'],
            # 'player_usg_percent': player['usg_percent'],
            # 'player_ts_prev': player['ts_percent'],
            'player_ast_prev': player['ast_percent'],
            # 'player_tov_prev': player['tov_percent'],
            'player_aortg' : player['aortg'],
            'player_adrtg' : player['adrtg'],
            'player_dreb_prev': player['dreb_percent'],
            # 'player_oreb_prev': player['oreb_percent'],
            'player_height': player['height_inches'],
            'prev_team_barthag_rank': player['prev_team_barthag_rank'],
            'next_team_barthag' : incoming_team_barthag_from_last_season,
            'next_team_adjoe' : incoming_team_adjoe_from_last_season,
            'next_team_adjde' : incoming_team_adjde_from_last_season,
            # 'prev_team_adjoe' : player['team_adj_off'],
            # 'next_team_adjde' : player['team_adj_def'],
            # 'team_eFG': player['team_eFG'],
            'avg_teammate_bpm': avg_teammate_bpm,
            'avg_teammate_usg': avg_teammate_usg,
            'avg_teammate_efg': avg_teammate_efg,
            # 'avg_teammate_two_percent': avg_teammate_2ptPercent,
            # 'avg_teammate_three_percent': avg_teammate_3ptPercent,
            # 'avg_teammate_ast': avg_teammate_ast,
            # 'avg_teammate_dreb': (teammates['dreb_percent'] * teammates['total_player_minutes']).sum() / total_minutes,
            # 'avg_teammate_oreb': (teammates['oreb_percent'] * teammates['total_minutes']).sum() / total_minutes,
            # 'avg_teammate_tov': (teammates['tov_percent'] * teammates['total_minutes']).sum() / total_minutes,
            # 'avg_teammate_ts': (teammates['ts_percent'] * teammates['total_minutes']).sum() / total_minutes,
            # 'avg_teammate_height': (teammates['height_inches'] * teammates['total_minutes']).sum() / total_minutes,            
            # 'teammates_minutes_total': total_minutes,
            # 'teammates_count': len(teammates),
            'rel_bpm': rel_bpm,
            'rel_usg': rel_usg,
            'rel_efg': rel_efg,
            'rel_ast': rel_ast,
            'rel_adjt': rel_adjt,
            # 'rel_two_percent': rel_two_percent,
            # 'rel_three_percent': rel_three_percent,
            'bpm_positive': int(bpm_to_predict > 0),  # target value for incoming season BPM
            # 'delta_bpm' : bpm_to_predict - player['bpm'],
            # 'next_year_usg_rate' : next_year_usg_rate,
            'player_name': player['player_name_x']  # for display/debugging
        }


        feature_rows.append(row)

# Build DataFrame
minimal_df = pd.DataFrame(feature_rows)

# Store player names separately
X_names = minimal_df['player_name']
X_ids = minimal_df['player_id']
X_years = minimal_df['prev_year']
# Train/test split
X = minimal_df.drop(columns=['bpm_positive', 'player_name', 'player_id','prev_year'])
y = minimal_df['bpm_positive']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = XGBClassifier()
model.fit(X_train, y_train)

# Evaluate
preds = model.predict(X_test)
probs = model.predict_proba(X_test)[:, 1]  # Probability of positive BPM
print("Classification Report:")
print(classification_report(y_test, preds))
print("Confusion Matrix:")
print(confusion_matrix(y_test, preds))
print("Accuracy:", accuracy_score(y_test, preds))

results_df = pd.DataFrame({
    'player_name': X_names.loc[X_test.index],
    'prev_year': X_years.loc[X_test.index],
    'actual_label': y_test.values,
    'predicted_label': preds,
    'prob_positive_bpm': probs
})

# Sort by confidence in fit
top_fit_players = results_df.sort_values('prob_positive_bpm', ascending=False)

confident_positives = results_df[results_df['prob_positive_bpm'] > 0.8]
uncertain = results_df[(results_df['prob_positive_bpm'] > 0.45) & (results_df['prob_positive_bpm'] < 0.55)]

print(confident_positives.head(10))
print(len(uncertain))