from queries import gptTransferQuery, statsFromPreviousSeason, playerRostersIncomingSeason
import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import LabelEncoder
conn = sqlite3.connect('rosteriq.db')

feature_rows = []

for year in range(2023, 2024):
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
        on='player_id'
    )


    label_encoder = LabelEncoder()
    statsFromTransferPlayersPrevSeasonDF['position_encoded'] = label_encoder.fit_transform(
        statsFromTransferPlayersPrevSeasonDF['position_x']
    )

    
    
    for index, player in statsFromTransferPlayersPrevSeasonDF.iterrows():
        player_id = player['player_id']
        new_team = player['new_team']
        bpm_to_predict = statsFromPlayersWhoHadAPreviousSeasonDF[
            statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] == player_id
            ]['bpm_to_predict'].values[0]
        
        if pd.isna(bpm_to_predict):
            print(f"Skipping player {player['player_name_x']} due to missing bpm_to_predict.")
            continue

        # Get incoming teammates (who had a previous season)
        teammates = statsFromPlayersWhoHadAPreviousSeasonDF[
            (statsFromPlayersWhoHadAPreviousSeasonDF['next_team_name'] == new_team) &
            (statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] != player_id)
        ]

        if teammates.empty:
            continue

        # Weighted averages for teammates
        total_minutes = teammates['total_minutes'].sum()
        avg_teammate_bpm = (teammates['bpm'] * teammates['total_minutes']).sum() / total_minutes
        avg_teammate_usg = (teammates['usg_percent'] * teammates['total_minutes']).sum() / total_minutes
        avg_teammate_efg = (teammates['efg_percent'] * teammates['total_minutes']).sum() / total_minutes
        avg_teammate_ast = (teammates['ast_percent'] * teammates['total_minutes']).sum() / total_minutes

        # Relative (role fit) features
        rel_bpm = player['bpm'] - avg_teammate_bpm
        rel_usg = player['usg_percent'] - avg_teammate_usg
        rel_efg = player['efg_percent'] - avg_teammate_efg
        rel_ast = player['ast_percent'] - avg_teammate_ast

        # Build feature row
        row = {    
            'player_position' : player['position_encoded'],     
            'player_'           
            # 'player_bpm_prev': player['bpm'],
            'player_usg_percent': player['usg_percent'],
            'player_efg_prev': player['efg_percent'],
            'player_ast_prev': player['ast_percent'],
            'player_tov_prev': player['tov_percent'],
            'player_height': player['height_inches'],
            'prev_team_barthag_rank': player['prev_team_barthag_rank'],
            'team_eFG': player['team_eFG'],
            'avg_teammate_bpm': avg_teammate_bpm,
            'avg_teammate_usg': avg_teammate_usg,
            'avg_teammate_efg': avg_teammate_efg,
            'avg_teammate_ast': avg_teammate_ast,
            'rel_bpm': rel_bpm,
            'rel_usg': rel_usg,
            'rel_efg': rel_efg,
            'rel_ast': rel_ast,
            'bpm_to_predict': bpm_to_predict,  # target value for incoming season BPM
            'player_name': player['player_name_x']  # for display/debugging
        }
        
        feature_rows.append(row)

print("Out the loop")
# Build DataFrame
minimal_df = pd.DataFrame(feature_rows)

# Store player names separately
X_names = minimal_df['player_name']
# Train/test split
X = minimal_df.drop(columns=['bpm_to_predict', 'player_name'])
y = minimal_df['bpm_to_predict']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = XGBRegressor()
model.fit(X_train, y_train)

# Evaluate
preds = model.predict(X_test)
print("Predictions vs Actual BPM:")
for actual, pred, name in zip(y_test, preds, X_names[X_test.index]):
    print(f"Player: {name}, Actual: {actual:.2f}, Predicted: {pred:.2f}")
mae = mean_absolute_error(y_test, preds)
print(f"MAE on minimal feature set: {mae:.2f}")
print("R^2", model.score(X_test, y_test))

# import shap
# import matplotlib.pyplot as plt

# # Create a SHAP explainer for your XGBoost model
# explainer = shap.Explainer(model)
# shap_values = explainer(X_test)

# # Summary plot: shows the impact of each feature across all predictions
# shap.summary_plot(shap_values, X_test, plot_type="bar")

# # For a deeper dive, visualize a single prediction (e.g., the first sample)
# def get_player_index(player_name, df):
#     # Find indices where the 'player_name' column matches the input
#     matching_indices = df.index[df['player_name'] == player_name].tolist()
#     if matching_indices:
#         return matching_indices[0]  # return the first match
#     else:
#         return None

# # Example usage:
# player_name_input = "Dajuan Clayton"
# index_of_player = get_player_index(player_name_input, minimal_df)
# if index_of_player is not None:
#     print(f"Index for player '{player_name_input}': {index_of_player}")
# else:
#     print(f"Player '{player_name_input}' not found in the dataset.")
# player_index = get_player_index(player_name_input, X_test)
# print(f"Explaining prediction for: {X_names.iloc[player_index]}")
# shap.plots.waterfall(shap_values[player_index])
# plt.show()
