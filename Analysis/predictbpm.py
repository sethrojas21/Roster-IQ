from queries import gptTransferQuery, statsFromPreviousSeason, playerRostersIncomingSeason
import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error

conn = sqlite3.connect('rosteriq.db')

feature_rows = []

for year in range(2020, 2024):
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

    for index, player in statsFromTransferPlayersPrevSeasonDF.iterrows():
        player_id = player['player_id']
        new_team = player['new_team']

        # Get incoming teammates (who had a previous season)
        teammates = statsFromPlayersWhoHadAPreviousSeasonDF[
            (statsFromPlayersWhoHadAPreviousSeasonDF['next_team_name'] == new_team) &
            (statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] != player_id)
        ]

        if teammates.empty:
            continue

        # Weighted averages
        total_minutes = teammates['total_minutes'].sum()
        avg_teammate_bpm = (teammates['bpm'] * teammates['total_minutes']).sum() / total_minutes
        avg_teammate_usg = (teammates['usg_percent'] * teammates['total_minutes']).sum() / total_minutes

        # Relative features
        rel_bpm = player['bpm'] - avg_teammate_bpm
        rel_usg = player['usg_percent'] - avg_teammate_usg

        row = {
            'player_bpm_prev': player['bpm'],
            'player_usg_percent': player['usg_percent'],
            'avg_teammate_bpm': avg_teammate_bpm,
            'avg_teammate_usg': avg_teammate_usg,
            'rel_bpm': rel_bpm,
            'rel_usg': rel_usg,
            'bpm_incoming': player['bpm'],  # target
            'player_name': player['player_name_x']  # new key added
        }
        
        feature_rows.append(row)

# Build DataFrame
minimal_df = pd.DataFrame(feature_rows)

# Store player names separately
X_names = minimal_df['player_name']
# Train/test split
X = minimal_df.drop(columns=['bpm_incoming', 'player_name'])
y = minimal_df['bpm_incoming']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = XGBRegressor()
model.fit(X_train, y_train)

# Evaluate
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)
print(f"MAE on minimal feature set: {mae:.2f}")
print("Predictions vs Actual BPM:")
for actual, pred, name in zip(y_test, preds, X_names[X_test.index]):
    print(f"Player: {name}, Actual: {actual:.2f}, Predicted: {pred:.2f}")
