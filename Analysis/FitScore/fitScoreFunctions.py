from enum import Enum
import sqlite3
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity

class Position(Enum):
    GUARD = "G"
    FOWARD = "F"
    CENTER = "C"

def get_standardized_player_rate_stats(conn, year, cluster_num, pos : str, normalized = True):
    rate_stats_df = pd.read_sql(
        f"""
        SELECT 
            ps.player_id,
            p.player_name,
            ps.season_year,               
            ps.efg_percent,
            ps.ast_percent,
            ps.oreb_percent,
            ps.dreb_percent,
            ps.tov_percent,
            ps.ft_percent,        
            ps.stl_percent,
            ps.blk_percent,
            (ps.threeA / ps.FGA) AS threeRate
        FROM Player_Seasons ps
        JOIN Team_Seasons ts
            ON ps.team_name = ts.team_name AND ps.season_year = ts.season_year
        JOIN Players p
            ON ps.player_id = p.player_id
        WHERE ps.season_year < ? AND ts.cluster = ? AND ps.position = ?
        """, conn, params= (year, cluster_num, pos)
    )

    columns = rate_stats_df.columns[3:]    

    df = rate_stats_df.copy()

    # scale only the rate-stat columns if requested
    scaler = None
    if normalized:
        scaler = StandardScaler().fit(df[columns])
        scaled_vals = scaler.transform(df[columns])
        # overwrite only the rate columns with their scaled versions
        df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    
    return (df, scaler)

def get_median_rate_stats_df(median_players_df, conn, year, cluster_num, pos: Position, normalized = True):
    columns = median_players_df.columns[3:]
    median_df = pd.DataFrame(columns=columns)
    median_data = []

    for column in columns:
        median_data.append(median_players_df[column].median())
    
    median_df.loc[len(median_df)] = median_data
    return median_df

def get_similar_players(median_vals: pd.DataFrame, players_median_stats: pd.DataFrame, k=5):
    # extract stat columns
    features = median_vals.columns.tolist()

    # compute similarity for each player
    players = players_median_stats.copy()
    # drop any rows with NaN in feature columns to avoid errors in similarity
    players = players.dropna(subset=features)
    sim_scores = cosine_similarity(
        players[features].values,
        median_vals[features].values.reshape(1, -1)
    ).flatten()
    players['similarity'] = sim_scores

    # sort descending and return top k players with IDs
    topk = players.sort_values('similarity', ascending=False).head(k)
    return topk[['player_id', 'season_year'] + features + ['similarity']]

def get_player_similarity_score(
        player_stats_df: pd.DataFrame,
        scaler: StandardScaler,
        columns: list,
        median_vals: pd.DataFrame
) -> float:
    """
    Apply an existing fitted StandardScaler to the given player_stats_df
    on the specified rate-stat columns and compute cosine similarity with median_vals.
    """
    # work on a copy
    df = player_stats_df.copy()
    # transform only the rate columns
    scaled_vals = scaler.transform(df[columns])
    # overwrite those columns with their z-scores
    df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    # compute cosine similarity between the single-player row and the median vector
    player_vec = df[columns].values.reshape(1, -1)
    median_vec = median_vals[columns].values.reshape(1, -1)
    score = float(cosine_similarity(player_vec, median_vec)[0, 0])
    return score

def testing():
    conn = sqlite3.connect('rosteriq.db')
    year, cluster_num, pos = 2021, 1, "G"
    standardized_player_df, scaler = get_standardized_player_rate_stats(conn, year, cluster_num, pos)
    median_vals_df = get_median_rate_stats_df(standardized_player_df, conn, year, cluster_num, pos)

    query = \
        """
        SELECT 
            ps.player_id,
            p.player_name,
            ps.season_year,               
            ps.efg_percent,
            ps.ast_percent,
            ps.oreb_percent,
            ps.dreb_percent,
            ps.tov_percent,
            ps.ft_percent,        
            ps.stl_percent,
            ps.blk_percent,
            (ps.threeA / ps.FGA) AS threeRate
        FROM Player_Seasons ps
        JOIN Players p ON ps.player_id = p.player_id
        WHERE ps.player_id = ? AND season_year = ?
        """
    aaron_cook_df = pd.read_sql(query, conn, params=(49449, 2020))

    aaron_cook_sim_score = get_player_similarity_score(aaron_cook_df, scaler, median_vals_df.columns, median_vals_df)

    print(aaron_cook_sim_score)
