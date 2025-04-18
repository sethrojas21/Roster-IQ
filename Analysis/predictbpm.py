from queries import gptTransferQuery, statsFromPreviousSeason, playerRostersIncomingSeason
import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import LabelEncoder
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


    label_encoder = LabelEncoder()
    statsFromTransferPlayersPrevSeasonDF['position_encoded'] = label_encoder.fit_transform(
        statsFromTransferPlayersPrevSeasonDF['position_x']
    )
    statsFromTransferPlayersPrevSeasonDF['team_encoded'] = label_encoder.fit_transform(
        statsFromTransferPlayersPrevSeasonDF['old_team']
    )
    
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
            'player_position' : player['position_encoded'],
            'prev_year': year,
            # 'prev_team_cluster' : player['prev_team_cluster'],
            # 'team_name': player['team_encoded'],              
            # 'player_bpm_prev': player['bpm'],
            'percentOfTeamMinutes' : player['total_player_minutes'] / player['total_team_minutes'],
            # 'player_usg_percent': player['usg_percent'],
            # 'player_ts_prev': player['ts_percent'],
            'player_ast_prev': player['ast_percent'],
            # 'player_tov_prev': player['tov_percent'],
            # 'player_adrtg' : player['adrtg'],
            # 'player_aortg' : player['aortg'],
            'player_dreb_prev': player['dreb_percent'],
            'player_height': player['height_inches'],
            # 'prev_team_barthag_rank': player['prev_team_barthag_rank'],
            'next_team_barthag' : incoming_team_barthag_from_last_season,
            'team_eFG': player['team_eFG'],
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
            # 'rel_adjt': rel_adjt,
            # 'rel_two_percent': rel_two_percent,
            # 'rel_three_percent': rel_three_percent,
            'bpm_to_predict': bpm_to_predict,  # target value for incoming season BPM
            # 'delta_bpm' : bpm_to_predict - player['bpm'],
            # 'next_year_usg_rate' : next_year_usg_rate,
            'player_name': player['player_name_x']  # for display/debugging
        }


        feature_rows.append(row)

# Build DataFrame
minimal_df = pd.DataFrame(feature_rows)

# import seaborn as sns
import matplotlib.pyplot as plt

# # Choose features for correlation analysis
# features_to_include = [
#     'player_ast_prev',
#     'player_dreb_prev',
#     'player_height',
#     'next_team_barthag',
#     'team_eFG',
#     'avg_teammate_bpm',
#     'avg_teammate_usg',
#     'avg_teammate_efg',
#     'rel_bpm',
#     'rel_usg',
#     'rel_efg',
#     'rel_ast',
#     'bpm_to_predict'
# ]

# # Compute and plot correlation matrix
# corr_df = minimal_df[features_to_include].corr()
# plt.figure(figsize=(12, 8))
# sns.heatmap(corr_df, annot=True, cmap='coolwarm', center=0)
# plt.title("Feature Correlation Matrix (with bpm_to_predict)")
# plt.xticks(rotation=45)
# plt.yticks(rotation=0)
# plt.tight_layout()
# plt.show()

# Visualize one decision tree from the XGBoost model

# # One-hot encode the prev_team_cluster column
# minimal_df = pd.get_dummies(minimal_df, columns=['prev_team_cluster'], prefix='cluster')

# Store player names separately
X_names = minimal_df['player_name']
X_ids = minimal_df['player_id']
X_years = minimal_df['prev_year']
# Train/test split
X = minimal_df.drop(columns=['bpm_to_predict', 'player_name', 'player_id','prev_year'])
y = minimal_df['bpm_to_predict']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = XGBRegressor(
    n_estimators=1000,
    learning_rate=0.01,
    max_depth=4,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42
)
model.fit(X_train, y_train)

from xgboost import plot_tree

# Plot the first tree (you can change the number to see different trees)
plt.figure(figsize=(40, 20))
plot_tree(model, num_trees=0, rankdir='LR')
plt.title("XGBoost - Tree 0 Visualization")
plt.tight_layout()
# plt.show()

# Evaluate
preds = model.predict(X_test)
print("Predictions vs Actual BPM:")
for actual, pred, name in zip(y_test, preds, X_names[X_test.index]):
    print(f"Player: {name}, Actual: {actual:.2f}, Predicted: {pred:.2f}")
    
mae = mean_absolute_error(y_test, preds)
print(f"MAE on minimal feature set: {mae:.2f}")
print("R^2", model.score(X_test, y_test))
 
# Error Analysis
import numpy as np
import matplotlib.pyplot as plt
import xgboost as xgb

# Compute residual errors for the test set
errors = y_test - preds
abs_errors = np.abs(errors)
 
print("\nError Analysis:")
print(f"Mean error: {np.mean(errors):.2f}")
print(f"Median error: {np.median(errors):.2f}")
print(f"Standard Deviation of error: {np.std(errors):.2f}")
 

# Plot feature importance
xgb.plot_importance(model, max_num_features=20, importance_type='gain')
plt.title('Top 20 Feature Importances by Gain')
plt.tight_layout()
# plt.show()

# Create a DataFrame combining predictions and actual values
test_df = X_test.copy()
test_df['Actual_BPM'] = y_test.values
test_df['Predicted_BPM'] = preds
test_df['Error'] = errors
test_df['Abs_Error'] = abs_errors
 
# Add player names to the DataFrame using X_names from the test set
test_players = X_names.loc[X_test.index]
test_ids = X_ids.loc[X_test.index]
test_prev_year = X_years.loc[X_test.index]
test_df['player_name'] = test_players
test_df['player_id'] = test_ids
test_df['prev_year'] = test_prev_year
 
print("Top 5 Players with highest absolute error:")
print(test_df[['player_name', 'Actual_BPM', 'Predicted_BPM', 'Error', 'Abs_Error']].sort_values('Abs_Error', ascending=False).head())
 
# Plot the distribution of prediction errors
plt.figure(figsize=(8, 6))
plt.hist(errors, bins=20, edgecolor='black')
plt.title("Distribution of Prediction Errors")
plt.xlabel("Prediction Error (Actual - Predicted)")
plt.ylabel("Frequency")
# plt.show()

# Identify players with prediction error outside of ±2.5
outliers_df = test_df[(test_df['Error'] < -2.5) | (test_df['Error'] > 2.5)]
# print(outliers_df)
import pickle
with open('Analysis/outliersDF.pkl', 'wb') as file:
    pickle.dump(outliers_df, file)
print(outliers_df[['player_name', 'Actual_BPM', 'Predicted_BPM', 'Error', 'Abs_Error']])
print(len(outliers_df) / len(X_test))
