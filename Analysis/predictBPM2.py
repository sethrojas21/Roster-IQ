import sqlite3
import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder

statsFromPreviousSeason = """
WITH TeamMinutesPlayed AS (
    SELECT 
        ps.team_name,
        ps.season_year,
        SUM(ps.min_pg * ps.games_played) AS total_team_minutes
    FROM Player_Seasons ps
    GROUP BY ps.team_name, ps.season_year
),

Player3PTFreq AS (
    SELECT
        ps.player_id,
        ps.season_year,
        ps.threeA * 1.0 / ps.FGA AS three_freq
    FROM Player_Seasons ps
),

League3PTFreq AS (
    SELECT 
        ps.season_year,
        SUM(ps.threeA) * 1.0 / SUM(ps.FGA) AS league_3pt_freq
    FROM Player_Seasons ps
    GROUP BY ps.season_year
),

LeagueAdjoe AS (
    SELECT 
        ts.season_year,
        AVG(ts.adjoe) AS league_avg_adjoe
    FROM Team_Seasons ts
    GROUP BY ts.season_year
)

SELECT
    ps.player_id,
    p.player_name,
    ps.player_year,
    ps.team_name AS prev_team_name,
    ts.barthag_rank AS prev_team_barthag_rank, -- Barthag rank of the player's previous team
    ps.height_inches,
    ps.position,
    ps.bpm,
    ps.games_played,
    ps.min_pg * ps.games_played AS total_minutes,
    tmp.total_team_minutes,
    ps.efg_percent, 
    ps.ts_percent, 
    ps.usg_percent, 
    ps.oreb_percent, 
    ps.dreb_percent, 
    ps.ast_percent, 
    ps.tov_percent,
    ts.eFG AS team_eFG,
    (ts.adjoe - ts.adjde) AS team_adj_netrg,
    ts.adjoe AS team_adj_off,
    p3freq.three_freq,
    l3freq.league_3pt_freq,
    ladjoe.league_avg_adjoe
FROM Player_Seasons ps
JOIN TeamMinutesPlayed tmp
    ON ps.team_name = tmp.team_name
    AND ps.season_year = tmp.season_year
JOIN Team_Seasons ts
    ON ps.team_name = ts.team_name
    AND ps.season_year = ts.season_year
JOIN Player3PTFreq p3freq
    ON ps.player_id = p3freq.player_id
    AND ps.season_year = p3freq.season_year
JOIN Players p
    ON ps.player_id = p.player_id
JOIN League3PTFreq l3freq
    ON ps.season_year = l3freq.season_year
JOIN LeagueAdjoe ladjoe
    ON ps.season_year = ladjoe.season_year
WHERE ps.season_year = ?
AND total_minutes > 100;
"""

playerRostersIncomingSeason = \
"""
SELECT
    ps.player_id,
    p.player_name,
    ps.team_name AS next_team_name,
    ts.barthag_rank AS next_team_barthag_rank -- Barthag rank of the team the player is going to
FROM Player_Seasons ps
JOIN Players p
    ON ps.player_id = p.player_id
JOIN Team_Seasons ts
    ON ps.team_name = ts.team_name
    AND ps.season_year = ts.season_year
WHERE ps.season_year = ?;
"""

conn = sqlite3.connect('rosteriq.db')


def predictBasedOnPlayerYear(year):
    # Load data for the previous season and incoming roster
    prevSznStat_df = pd.read_sql_query(statsFromPreviousSeason, conn, params=(year,))
    incomingRoster_df = pd.read_sql_query(playerRostersIncomingSeason, conn, params=(year + 1,))

    distinctTeams = incomingRoster_df['next_team_name'].unique()

    # Initialize empty DataFrames for combined training data
    combined_X = pd.DataFrame()
    combined_Y = pd.Series(dtype=float)

    # Encode the position column
    label_encoder = LabelEncoder()
    prevSznStat_df['position_encoded'] = label_encoder.fit_transform(prevSznStat_df['position'])

    for team in distinctTeams:
        # Get the players for the current/incoming team that had a season last year
        players_ids = incomingRoster_df[incomingRoster_df['next_team_name'] == team]['player_id'].tolist()

        # Get player stats from the previous season for the current team's roster
        df_team = prevSznStat_df[prevSznStat_df['player_id'].isin(players_ids)]

        print(f"Processing team: {team}")
        # print("Team roster from the previous season:")
        # print(df_team)

        # Prepare the dataset for training
        X = df_team.drop(columns=['bpm', 'player_id', 'player_name', 'prev_team_name', 'position'])
        Y = df_team['bpm']

        # Append the team's data to the combined dataset
        combined_X = pd.concat([combined_X, X], ignore_index=True)
        combined_Y = pd.concat([combined_Y, Y], ignore_index=True)

    # Split the combined data into training and testing sets
    X_train, X_test, Y_train, Y_test = train_test_split(combined_X, combined_Y, test_size=0.2, random_state=42)

    # Train the XGBoost model
    model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=6, random_state=42)
    model.fit(X_train, Y_train)

    # Make predictions
    Y_pred = model.predict(X_test)

    # Evaluate the model
    mae = mean_absolute_error(Y_test, Y_pred)
    r2 = r2_score(Y_test, Y_pred)

    print(f"Overall Model Performance for Year {year}:")
    print(f"Mean Absolute Error (MAE): {mae:.2f}")
    print(f"R² Score: {r2:.2f}")
    print("-" * 40)

    # Feature importance (optional)
    feature_importances = model.feature_importances_
    feature_names = combined_X.columns
    print("Feature Importances:")
    for name, importance in zip(feature_names, feature_importances):
        print(f"{name}: {importance:.4f}")


def predictBasedOnTeamYear(year):
    prevSznStat_df = pd.read_sql_query(statsFromPreviousSeason, conn, params=(year,))
    incomingRoster_df = pd.read_sql_query(playerRostersIncomingSeason, conn, params=(year+1,))

    distinctTeams = incomingRoster_df['team_name'].unique()

    for team in distinctTeams[:3]:
        # Get the players for the current/incoming team
        players_ids = incomingRoster_df[incomingRoster_df['team_name'] == team]['player_id'].tolist()
        print(players_ids)

        # Get player stats from the previous season for the current team's roster
        df_team = prevSznStat_df[prevSznStat_df['player_id'].isin(players_ids)]
        
        print(f"Processing team: {team}")
        print("Team roster from the previous season:")
        print(df_team)
        for player_id in players_ids:
            # Store the player being removed
            removed_player = df_team[df_team['player_id'] == player_id]

            # Create a new DataFrame without the removed player
            df_team_without_player = df_team[df_team['player_id'] != player_id]

            # Print the resulting DataFrame
            print(f"\nTeam without player {removed_player['player_name']}:")
            print(df_team_without_player)

            # Optionally, you can store or process `removed_player` here
            print(f"Removed player: {removed_player['player_name']}")

        print("-" * 40)
        break  # Remove this `break` if you want to process all teams


def predictBasedOnPlayerYears(start_year, end_year):
    # Initialize empty DataFrames for combined training data
    combined_X = pd.DataFrame()
    combined_Y = pd.Series(dtype=float)

    for year in range(start_year, end_year + 1):
        # Load data for the previous season
        prevSznStat_df = pd.read_sql_query(statsFromPreviousSeason, conn, params=(year,))

        # Encode the position column
        label_encoder = LabelEncoder()
        prevSznStat_df['position_encoded'] = label_encoder.fit_transform(prevSznStat_df['position'])

        # Prepare the dataset for training
        X = prevSznStat_df.drop(columns=['bpm', 'player_id', 'player_name', 'prev_team_name', 'position'])
        Y = prevSznStat_df['bpm']

        # Append the year's data to the combined dataset
        combined_X = pd.concat([combined_X, X], ignore_index=True)
        combined_Y = pd.concat([combined_Y, Y], ignore_index=True)

    # Split the combined data into training and testing sets
    X_train, X_test, Y_train, Y_test = train_test_split(combined_X, combined_Y, test_size=0.2, random_state=42)

    # Train the XGBoost model
    model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=6, random_state=42)
    model.fit(X_train, Y_train)

    # Make predictions
    Y_pred = model.predict(X_test)

    # Evaluate the model
    mae = mean_absolute_error(Y_test, Y_pred)
    r2 = r2_score(Y_test, Y_pred)

    print(f"Overall Model Performance for Years {start_year}-{end_year}:")
    print(f"Mean Absolute Error (MAE): {mae:.2f}")
    print(f"R² Score: {r2:.2f}")
    print("-" * 40)

    # Feature importance (optional)
    feature_importances = model.feature_importances_
    feature_names = combined_X.columns
    print("Feature Importances:")
    for name, importance in zip(feature_names, feature_importances):
        print(f"{name}: {importance:.4f}")


# Train the model over multiple years (e.g., 2020 to 2024)
predictBasedOnPlayerYears(start_year=2020,end_year=2024)