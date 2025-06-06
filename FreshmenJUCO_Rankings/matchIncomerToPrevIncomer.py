import pandas as pd
import sqlite3
from Analysis.queries import players_info_query, hs_query, team_seasons_barthag_query
from Database.addPlayerWeightToDB import get_best_match
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import euclidean_distances
import numpy as np

conn = sqlite3.connect('rosteriq.db')
cursor = conn.cursor()

def join_espn_rankings_to_players():
    # Get player info not containing their hs rankings

    players_info_df = pd.read_sql(players_info_query, conn)
    hs_df = pd.read_sql(hs_query, conn)
    hs_df = hs_df[hs_df['season_year'] < 2024]
    ts_barthag_df = pd.read_sql(team_seasons_barthag_query, conn)
    
    player_names = set(players_info_df['player_name'])
    hs_names = set(hs_df['player_name'])
    
    
    # Update name
    for hs_player_name in hs_names:
        best_match_name = get_best_match(hs_player_name, player_names, score_cutoff=70)
        
        if best_match_name is not None:
            hs_df.loc[hs_df['player_name'] == hs_player_name, 'player_name'] = best_match_name
        else:
            print(hs_player_name)
    
    # Join all dfs to get max players_info_query
    hs_df_merged = hs_df.merge(players_info_df, on=["player_name", "team_name"], how = "left")
    return hs_df_merged

def get_hs_w_barthag_df():
    hs_df = pd.read_sql(hs_query, conn)
    ts_barthag_df = pd.read_sql(team_seasons_barthag_query, conn)

    df_merged = hs_df.merge(ts_barthag_df, on = ["team_name", "season_year"])
    return df_merged

def get_hs_players_actual_stats(player_df):
    if player_df.empty:
        return pd.DataFrame()

    # Get player IDs
    unique_names = tuple(player_df['player_name'].unique())
    name_placeholders = ",".join(["?"] * len(unique_names))
    id_query = f"SELECT player_id, player_name FROM Players WHERE player_name IN ({name_placeholders})"
    id_df = pd.read_sql(id_query, conn, params=list(unique_names))

    # Merge player_id into original df
    merged_df = player_df.merge(id_df, on="player_name")
    merged_df['season_year'] += 1  # get next season

    conditions = " OR ".join(["(p.player_id = ? AND ps.season_year = ?)"] * len(merged_df))
    sql = f"""
        SELECT 
            ps.player_id,
            p.player_name,
            ps.bpm,
            ps.pts_pg,
            ps.min_pg,
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
            ps.POSS,
            (ps.OREB / ps.POSS) * 100 AS oreb_100,
            (ps.DREB / ps.POSS) * 100 AS dreb_100,
            (ps.PTS / ps.POSS) * 100 AS pts_100
        FROM Player_Seasons ps
        JOIN Players p ON ps.player_id = p.player_id
        WHERE {conditions}
    """
    flattened_params = [item for pair in zip(merged_df['player_id'], merged_df['season_year']) for item in pair]
    return pd.read_sql(sql, conn, params=flattened_params)

def match_incomer_with_prev_incomer(player_stats_v, prev_incomer_matrix, prev_incomer_df, k=7):
    distances = euclidean_distances([player_stats_v], prev_incomer_matrix)[0]
    top_k_idx = np.argsort(distances)[:k]
    return prev_incomer_df.iloc[top_k_idx].assign(distance=distances[top_k_idx])

def weighted_average_stats(matched_stats):
    # Avoid divide-by-zero by adding a small constant
    matched_stats = matched_stats.copy()
    matched_stats['weight'] = 1 / (matched_stats['distance'] + 1e-6)

    stat_cols = matched_stats.columns
    weighted_means = {}
    for col in stat_cols:
        if col not in matched_stats:
            continue
        if col in ["adjoe", "adjde"]:
            combined_weight = matched_stats['weight'] * matched_stats['POSS']
            weighted_means[col] = np.average(matched_stats[col], weights=combined_weight)
        else:
            weighted_means[col] = np.average(matched_stats[col], weights=matched_stats['weight'])
    
    # Derive per-game stats
    weighted_means['pts_100'] = weighted_means['PTS'] / weighted_means['POSS'] * 100
    weighted_means['oreb_100'] = weighted_means['OREB'] / weighted_means['POSS'] * 100
    weighted_means['dreb_100'] = weighted_means['DREB'] / weighted_means['POSS'] * 100

    return pd.Series(weighted_means)

df = pd.read_csv('FreshmenJUCO_Rankings/hsWithTS.csv')

scaler = StandardScaler()
position_encoded = pd.get_dummies(df['position'])
season_year = df[['season_year']]
names = df[['player_name', 'season_year']]
numeric_features = df[['height_inches', 'ranking', 'barthag']]
numeric_scaled = scaler.fit_transform(numeric_features)

X = np.hstack([numeric_scaled, position_encoded.values])

mask_2023 = season_year['season_year'] == 2023
mask_before_2023 = season_year['season_year'] < 2023

X_2023 = X[mask_2023.values]
X_before_2023 = X[mask_before_2023.values]

df_2023 = df[mask_2023].reset_index(drop=True)
df_before_2023 = df[mask_before_2023].reset_index(drop=True)

df_stats = get_hs_players_actual_stats(names)

for i, player_vec in enumerate(X_2023):
    player_name = df_2023.iloc[i]['player_name']
    print(f"\nMatching for 2023 Player {player_name}:")
    matched_players = match_incomer_with_prev_incomer(player_vec, X_before_2023, df_before_2023)
    matched_stats = pd.DataFrame(columns=df_stats.columns)
    matched_stats = pd.concat([ df_stats[df_stats['player_name'] == player['player_name']]
                                for _, player in matched_players[['player_name', 'season_year', 'distance']].iterrows()
                                ], ignore_index=True)
    print(matched_stats)
    merged_stats = matched_stats.merge(matched_players, on = ["player_name"])
    # print(merged_stats)
    wa_stats = weighted_average_stats(merged_stats[['bpm', "MIN", "OREB", "DREB", "POSS", "distance", "PTS", "adjoe", 'adjde']])
    df_result = wa_stats.to_frame().T
    cats_looking_at = ['bpm', 'pts_100', 'oreb_100', 'dreb_100', 'adjoe', 'adjde']
    print(df_result[cats_looking_at])
    print("Actual: ")
    actual_df = df_stats[df_stats['player_name'] == player_name][cats_looking_at]
    print(actual_df)
    try:
        difference_df = df_result[cats_looking_at] - actual_df[cats_looking_at].values
        print("DIFFERENCE")
        print(difference_df)
    except:
        print("Failed to find difference")
    if i > 25: break
    
