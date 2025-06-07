import pandas as pd
import sqlite3
from Analysis.queries import players_info_query, hs_query, team_seasons_barthag_query
from Database.addPlayerWeightToDB import get_best_match
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import euclidean_distances
import numpy as np

conn = sqlite3.connect('rosteriq.db')
cursor = conn.cursor()

def get_hs_players_actual_stats(player_df):
    if player_df.empty:
        return pd.DataFrame()

    # Get player IDs
    unique_names = list(player_df['player_name'].unique())   
    unique_names.remove("Jalen Johnson") 
    name_placeholders = ",".join(["?"] * len(unique_names))
    id_query = f"SELECT player_id, player_name FROM Players WHERE player_name IN ({name_placeholders})"
    id_df = pd.read_sql(id_query, conn, params=list(unique_names))

    # Merge player_id into original df
    merged_df = player_df.merge(id_df, on="player_name")
    merged_df['season_year'] += 1  # get next season

    conditions = " OR ".join(["(p.player_id = ? AND ps.season_year = ?)"] * len(merged_df))
    sql_query_no_where = f"""
        SELECT 
            ps.player_id,
            p.player_name,
            ps.bpm,                    
            ps.PTS,
            ps.adjoe,
            ps.adrtg AS adjde,
            ps.TOV,
            ps.STL,
            ps.FGM,
            ps.threeM AS P3M,
            ps.FGA,            
            ps.MIN,
            ps.OREB,
            ps.DREB,
            ps.POSS           
        FROM Player_Seasons ps
        JOIN Players p ON ps.player_id = p.player_id
    """
    flattened_params = [item for pair in zip(merged_df['player_id'], merged_df['season_year']) for item in pair]
    df = pd.read_sql(sql_query_no_where + f" WHERE {conditions}", conn, params=flattened_params)
    jalen_johnson_df = pd.read_sql(sql_query_no_where + " WHERE ps.player_id = 73238 AND ps.season_year = 2021", conn)
    
    # Ensure Jalen Johnson is included at the bottom
    df = pd.concat([df, jalen_johnson_df], ignore_index=True)
    
    return df
    

def estimate_freshman_production(incoming_player_vector, historical_freshmen_matrix, historical_freshmen_df, k=5):
    # Calculate Euclidean distances between incoming player and historical freshmen
    similarity_scores = euclidean_distances([incoming_player_vector], historical_freshmen_matrix)[0]
    # Find indices of k closest matches
    top_k_indices = np.argsort(similarity_scores)[:k]
    # Return top matched players with their similarity scores
    return historical_freshmen_df.iloc[top_k_indices].assign(distance=similarity_scores[top_k_indices])

def weighted_average_stats(estimated_stats):
    # Avoid divide-by-zero by adding a small constant
    estimated_stats = estimated_stats.copy()
    estimated_stats['weight'] = 1 / (estimated_stats['distance'] + 1e-6)

    stat_cols = estimated_stats.columns
    weighted_means = {}
    for col in stat_cols:
        if col not in estimated_stats:
            continue
        if col in ["adjoe", "adjde"]:
            combined_weight = estimated_stats['weight']
            weighted_means[col] = np.average(estimated_stats[col], weights=combined_weight)
        else:
            weighted_means[col] = np.average(estimated_stats[col], weights=estimated_stats['weight'])

    return pd.Series(weighted_means)

df = pd.read_csv('FreshmenJUCO_Rankings/hsWithTS.csv')
# Standardize numeric features
scaler = StandardScaler()
position_encoded = pd.get_dummies(df['position'])
season_year = df[['season_year']]
names = df[['player_name', 'season_year']]
numeric_features = df[['height_inches', 'ranking', 'barthag']]
numeric_scaled = scaler.fit_transform(numeric_features)

# Combine scaled numeric features with position encoding
X = np.hstack([numeric_scaled, position_encoded.values])

# Retrieve actual stats for players
df_stats = get_hs_players_actual_stats(names)

"""
Iterate through 2020-2024
Get their predicted statistics
Add it to the database
"""
for year in range(2020, 2024):
    # Create masks for the current season and seasons before current season
    mask_current_season = season_year['season_year'] == year
    mask_before_current = season_year['season_year'] < year

    # Split feature matrix according to masks
    X_current = X[mask_current_season.values]
    X_before_current = X[mask_before_current.values]

    # Split dataframes according to masks
    df_current = df[mask_current_season].reset_index(drop=True)
    df_before_current = df[mask_before_current].reset_index(drop=True)
    ind = 0
    for i, incoming_player_vector in enumerate(X_current):
        player_name = df_current.iloc[i]['player_name']
        print(f"\nMatching for Player from {year}: {player_name}")
        # Find top matches from historical freshmen
        top_matches = estimate_freshman_production(incoming_player_vector, X_before_current, df_before_current)
        # Gather actual stats for matched players
        estimated_stats = pd.DataFrame(columns=df_stats.columns)
        estimated_stats = pd.concat([ df_stats[df_stats['player_name'] == player['player_name']]
                                    for _, player in top_matches[['player_name', 'season_year', 'distance']].iterrows()
                                    ], ignore_index=True)
        print(estimated_stats)
        # Merge stats with distance information for weighting
        merged_stats = estimated_stats.merge(top_matches, on = ["player_name"])
        # Define stat categories to consider
        cats_looking_at = ['bpm', 'adjoe', 'adjde', 'TOV', 'STL', 'OREB', 'DREB', 'FGM', 'P3M', 'FGA', 'MIN']
        # Compute weighted average stats based on similarity scores
        wa_stats = weighted_average_stats(merged_stats[cats_looking_at + ['distance', 'POSS']])
        df_result = wa_stats.to_frame().T    
        predicted_player_results = df_result[cats_looking_at]
        # Ensure integer columns for database insertion
        int_cols = cats_looking_at[3:]
        predicted_player_results[int_cols] = predicted_player_results[int_cols].round().astype(int)
        print("Predicted: ")
        print(player_df_results)

        actual_df = df_stats[df_stats['player_name'] == player_name][cats_looking_at]
        print("Actual: ")
        print(actual_df)  
        # Add this to database        
        add_query = """UPDATE HS_Rankings
                    SET bpm = ?, adjoe = ?, adjde = ?, TOV = ?, STL = ?, OREB = ?, DREB = ?, FGM = ?, P3M = ?, FGA = ?, MIN = ?
                    WHERE player_name = ?""" 
        BPM = float(predicted_player_results['bpm'].iloc[0])       
        ADJOE = float(predicted_player_results['adjoe'].iloc[0])
        ADJDE = float(predicted_player_results['adjde'].iloc[0])        
        TOV = int(predicted_player_results['TOV'].iloc[0])        
        STL = int(predicted_player_results['STL'].iloc[0])        
        OREB = int(predicted_player_results['OREB'].iloc[0])        
        DREB = int(predicted_player_results['DREB'].iloc[0])        
        FGM = int(predicted_player_results['FGM'].iloc[0])
        P3M = int(predicted_player_results['P3M'].iloc[0])
        FGA = int(predicted_player_results['FGA'].iloc[0])
        MIN = int(predicted_player_results['MIN'].iloc[0])
        cursor.execute(add_query, (BPM, ADJOE, ADJDE, TOV, STL, OREB, DREB, FGM, P3M, FGA, MIN, player_name))

        if ind == 50:
            conn.commit()
            ind = 0
        else:
            ind += 1
    
    conn.commit()
