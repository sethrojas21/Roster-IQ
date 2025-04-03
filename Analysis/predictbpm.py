import pandas as pd
import numpy as np
import queries
import sqlite3
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

# Connect to the SQLite database
conn = sqlite3.connect("rosteriq.db")

# Query to get player stats for the current season with teammate averages
current_season_query = """
WITH TeammateStats AS (
    SELECT 
        ps.team_name,
        AVG(ps.bpm) AS teammate_avg_bpm,
        AVG(ps.efg_percent) AS teammate_avg_efg,
        AVG(ps.usg_percent) AS teammate_avg_usg,
        AVG(ps.ast_percent) AS teammate_avg_ast,
        AVG(ps.oreb_percent) AS teammate_avg_oreb,
        AVG(ps.dreb_percent) AS teammate_avg_dreb,
        AVG(ps.treb_pg) AS teammate_avg_treb
    FROM Player_Seasons ps
    WHERE ps.season_year = ?
    GROUP BY ps.team_name
)
SELECT 
    ps.player_id,
    ps.team_name AS current_team,
    ps.height_inches, 
    ps.weight_lbs, 
    ps.position,
    ps.efg_percent, 
    ps.ts_percent, 
    ps.usg_percent, 
    ps.oreb_percent, 
    ps.dreb_percent, 
    ps.ast_percent, 
    ps.tov_percent, 
    ps.ftr AS free_throw_rate,
    ps.ft_percent, 
    ps.three_percent AS three_point_percentage, 
    ps.blk_percent AS block_percentage, 
    ps.stl_percent AS steal_percentage,  
    ps.adjoe AS adjusted_offensive_efficiency, 
    ps.ast_tov_r AS assist_turnover_ratio, 
    ps.drtg AS defensive_rating, 
    ps.adrtg AS adjusted_defensive_rating, 
    ps.bpm AS current_bpm,
    ps.obpm AS offensive_box_plus_minus,
    ps.dbpm AS defensive_box_plus_minus,
    ts.pts_pg AS current_team_pts_pg,
    ts.oreb_pg AS current_team_oreb_pg,
    ts.dreb_pg AS current_team_dreb_pg,
    ts.treb_pg AS current_team_treb_pg,
    ts.ast_pg AS current_team_ast_pg,
    ts.stl_pg AS current_team_stl_pg,
    ts.blk_pg AS current_team_blk_pg,
    ts.eFG AS current_team_eFG,
    ts.ftr AS current_team_ftr,
    ts.three_percent AS current_team_three_percent,
    ts.ft_percent AS current_team_ft_percent,
    ts.adjoe AS current_team_adjoe,
    ts.con_adj_oe AS current_team_con_adj_oe,
    ts.con_adj_de AS current_team_con_adj_de,
    ts.conf_win_percent AS current_team_conf_win_percent,
    t.teammate_avg_bpm,
    t.teammate_avg_efg,
    t.teammate_avg_usg,
    t.teammate_avg_ast,
    t.teammate_avg_oreb,
    t.teammate_avg_dreb,
    t.teammate_avg_treb
FROM Player_Seasons ps
JOIN Team_Seasons ts
    ON ps.team_name = ts.team_name
    AND ps.season_year = ts.season_year
JOIN TeammateStats t
    ON ps.team_name = t.team_name
WHERE ps.season_year = ?;
"""

# Query to get next team's stats for the next season with teammate averages
next_team_query = """
WITH TeammateStats AS (
    SELECT 
        ps_next.team_name AS next_team,
        AVG(ps_prev.bpm) AS teammate_avg_bpm,
        AVG(ps_prev.efg_percent) AS teammate_avg_efg,
        AVG(ps_prev.usg_percent) AS teammate_avg_usg,
        AVG(ps_prev.ast_percent) AS teammate_avg_ast,
        AVG(ps_prev.oreb_percent) AS teammate_avg_oreb,
        AVG(ps_prev.dreb_percent) AS teammate_avg_dreb,
        AVG(ps_prev.treb_pg) AS teammate_avg_treb
    FROM Player_Seasons ps_next
    JOIN Player_Seasons ps_prev
        ON ps_next.player_id != ps_prev.player_id
        AND ps_next.season_year = ps_prev.season_year + 1
        AND ps_next.team_name = ps_prev.team_name
    WHERE ps_next.season_year = ?
    GROUP BY ps_next.team_name
)
SELECT 
    ps_next.player_id,
    ps_next.team_name AS next_team,
    ps_next.bpm AS next_season_bpm, -- Include next season BPM
    ps_prev.height_inches,
    ps_prev.weight_lbs,
    ps_prev.min_pg,
    ps_prev.pts_pg,
    ps_prev.ast_pg,
    ps_prev.oreb_pg,
    ps_prev.dreb_pg,
    ps_prev.treb_pg,
    ps_prev.stl_pg,
    ps_prev.blk_pg,
    ps_prev.efg_percent,
    ps_prev.ts_percent,
    ps_prev.usg_percent,
    ps_prev.oreb_percent,
    ps_prev.dreb_percent,
    ps_prev.ast_percent,
    ps_prev.tov_percent,
    ps_prev.ftr AS free_throw_rate,
    ps_prev.ft_percent,
    ps_prev.three_percent AS three_point_percentage,
    ps_prev.blk_percent AS block_percentage,
    ps_prev.stl_percent AS steal_percentage,
    ps_prev.adjoe AS adjusted_offensive_efficiency,
    ps_prev.ast_tov_r AS assist_turnover_ratio,
    ps_prev.drtg AS defensive_rating,
    ps_prev.adrtg AS adjusted_defensive_rating,
    ts_next.adjoe AS next_team_adjoe,
    ts_next.con_adj_oe AS next_team_con_adj_oe,
    ts_next.con_adj_de AS next_team_con_adj_de,
    ts_next.conf_win_percent AS next_team_conf_win_percent,
    t.teammate_avg_bpm,
    t.teammate_avg_efg,
    t.teammate_avg_usg,
    t.teammate_avg_ast,
    t.teammate_avg_oreb,
    t.teammate_avg_dreb,
    t.teammate_avg_treb
FROM Player_Seasons ps_next
JOIN Player_Seasons ps_prev
    ON ps_next.player_id = ps_prev.player_id
    AND ps_next.season_year = ps_prev.season_year + 1
JOIN Team_Seasons ts_next
    ON ps_next.team_name = ts_next.team_name
    AND ps_next.season_year = ts_next.season_year
JOIN TeammateStats t
    ON ps_next.team_name = t.next_team
WHERE ps_next.season_year = ?;
"""

# Define the current and next season years
current_season = 2022
next_season = 2023

# Load data for the current season
current_season_data = pd.read_sql(current_season_query, conn, params=(current_season, current_season))

# Load data for the next season
next_season_data = pd.read_sql(next_team_query, conn, params=(next_season, next_season))

# Merge the current season data with the next season data
data = pd.merge(current_season_data, next_season_data, on="player_id", how="inner")

# Drop unnecessary columns
columns_to_drop = ['player_id', 'current_team', 'next_team']
data = data.drop(columns=columns_to_drop, errors='ignore')

# Handle missing values
data = data.dropna()

# Define features (X) and target (y)
X = data.drop(columns=['next_season_bpm'])
y = data['next_season_bpm']

# Standardize the features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

# Train a Random Forest model
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Predict and evaluate
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Mean Absolute Error (MAE): {mae:.2f}")
print(f"R² Score: {r2:.2f}")

# Close the database connection
conn.close()