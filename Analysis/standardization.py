from enum import Enum
import sqlite3
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from dataLoader import load_players_from_cluster, load_players_from_multiple_clusters

column_shift = 7

def get_standardized_player_rate_stats(stat_query,conn, year, cluster_num, pos : str, normalized = True):        
    rate_stats_df = load_players_from_cluster(stat_query, conn, year, cluster_num, pos)        
    
    columns = rate_stats_df.columns[column_shift:]    

    df = rate_stats_df.copy()        

    # scale only the rate-stat columns if requested
    scaler = None
    if normalized:
        scaler = StandardScaler().fit(df[columns])
        scaled_vals = scaler.transform(df[columns])                
        # overwrite only the rate columns with their scaled versions                   
        df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    
    return (df, scaler)

def standardized_player_rate_stats(stat_query,conn, year, cluster_nums, pos : str, normalized = True):
    """
    New function that deals with weights of multiple clusters
    """
    rate_stats_df = load_players_from_multiple_clusters(stat_query, conn, year, cluster_nums, pos)    
    
    columns = rate_stats_df.columns[column_shift:-1]   # Exclude meta data and cluster_num

    df = rate_stats_df.copy()

    # scale only the rate-stat columns if requested
    scaler = None
    if normalized:
        scaler = StandardScaler().fit(df[columns])
        scaled_vals = scaler.transform(df[columns])                
        # overwrite only the rate columns with their scaled versions                   
        df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    
    return (df, scaler)

def get_nPercentile_rate_stats_df(nPercentile_players_df, percentile=0.5):
    columns = nPercentile_players_df.columns[column_shift:]
    nPercentile_df = pd.DataFrame(columns=columns)
    nPercentile_data = []

    for column in columns:
        nPercentile_data.append(nPercentile_players_df[column].quantile(percentile))
    
    nPercentile_df.loc[len(nPercentile_df)] = nPercentile_data
    return nPercentile_df

#TODO: move into get benchmark player file
def get_nPercentile_benchmark_stats(
        nPercentile_players_cluster_df: pd.DataFrame,
        cluster_weights: dict[int, float],
        percentile: float = 0.5,
        player_cluster_weights: dict[int, float] = None
) -> pd.DataFrame:
    if "cluster_num" not in nPercentile_players_cluster_df.columns:
        raise ValueError("Input df must include a 'cluster_num' column.")

    # Rate‑stat feature columns start after the first 4 metadata columns
    stat_cols = nPercentile_players_cluster_df.columns[column_shift:-1]

    # 1. Per‑cluster percentile rows (keep cluster_num as index)
    per_cluster = (
        nPercentile_players_cluster_df
        .groupby("cluster_num")[stat_cols]
        .quantile(percentile)
    )

    # 2. Attach weights using the index
    per_cluster["w"] = per_cluster.index.map(cluster_weights).fillna(0)

    # 3. Weighted blend
    blended_vec = (
        per_cluster[stat_cols]
        .multiply(per_cluster["w"], axis=0)
        .sum()
        .to_frame()
        .T
    )

    # Ensure the result has the same column order
    blended_vec = blended_vec[stat_cols]

    return blended_vec

def get_nPercentile_info(query, conn, year, cluster_num, pos : str, percentile = 0.5, normalized = True):
    df, scaler = get_standardized_player_rate_stats(query, conn, year, cluster_num, pos, normalized)
    return (scaler, get_nPercentile_rate_stats_df(df, percentile))

def get_nPercentile_scalar_and_vals(query, conn, year, cluster_weights, pos : str, percentile = 0.5, normalized = True):
    df, scaler = standardized_player_rate_stats(query, conn, year, list(cluster_weights.keys()), pos, normalized)
    filtered_df = filter_cluster_players(df, pos)    
    return (scaler, get_nPercentile_benchmark_stats(df, cluster_weights, percentile))

def get_nPercentile_scalar_and_vals_roles(query, conn, year, cluster_weights, pos : str, percentile = 0.5, normalized = True):
    df, scaler = standardized_player_rate_stats(query, conn, year, list(cluster_weights.keys()), pos, normalized)
    filtered_df = filter_cluster_players(df)    

    roles = ['bench', 'rotation', 'starter']
    # for k, v in cluster_weights.items():
    #     print(f"Cluster number: {k}", len(filtered_df[filtered_df['cluster_num'] == k]))
    
    bench_df = filtered_df[filtered_df['min_pg'] < 10]
    rotation_df = filtered_df[(filtered_df['min_pg'] >= 10) & (filtered_df['min_pg'] < 25)]
    starter_df = filtered_df[(filtered_df['min_pg'] >= 25)]
    roles_df_list = [bench_df, rotation_df, starter_df]

    role_dict = {}
    for i in range(len(roles)):   
        # print(f"{roles[i]}: {len(roles_df_list[i])}")     
        med_vals = get_nPercentile_benchmark_stats(roles_df_list[i], cluster_weights, percentile)
        role_dict[roles[i]] = med_vals    
    return (scaler, role_dict)


def scale_player_stats(
        player_stats_df: pd.DataFrame,
        scaler: StandardScaler,
        columns: list):
    # work on a copy
    df = player_stats_df.copy()
    # transform only the rate columns
    scaled_vals = scaler.transform(df[columns])
    # overwrite those columns with their z-scores
    df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    # compute cosine similarity between the single-player row and the median vector
    player_vec = df[columns].values.reshape(1, -1)
    return player_vec

#TODO: move into a similarity score file
def get_player_similarity_score(
        player_stats_df: pd.DataFrame,
        scaler: StandardScaler,
        columns: list,
        nPercentile_vals: pd.DataFrame
) -> float:
    """
    Apply an existing fitted StandardScaler to the given player_stats_df
    on the specified rate-stat columns and compute cosine similarity with median_vals.
    """
    scaled_player_vec = scale_player_stats(player_stats_df, scaler, columns)
    nPercentile_vec = nPercentile_vals[columns].values.reshape(1, -1)
    score = float(cosine_similarity(scaled_player_vec, nPercentile_vec)[0, 0])
    return score

#TODO: move into a get simlary playres file
def get_similar_players(nPercentile_vals: pd.DataFrame, players_median_stats: pd.DataFrame, k=5):
    # extract stat columns
    features = nPercentile_vals.columns.tolist()

    # compute similarity for each player
    players = players_median_stats.copy()
    # drop any rows with NaN in feature columns to avoid errors in similarity
    players = players.dropna(subset=features)
    sim_scores = cosine_similarity(
        players[features].values,
        nPercentile_vals[features].values.reshape(1, -1)
    ).flatten()
    players['similarity'] = sim_scores

    # sort descending and return top k players with IDs
    topk = players.sort_values('similarity', ascending=False).head(k)
    return topk[['player_id', 'season_year'] + features + ['similarity']]

def filter_cluster_players(df, winningTeams=False, bpm=True): 
    copy_df = df.copy()
    
    if winningTeams:       
        threshold = copy_df['barthag_rank'].quantile(0.5)
        copy_df = copy_df[copy_df['barthag_rank'] <= threshold]  

    if bpm:
        copy_df = copy_df[copy_df['bpm'] > 0]
    
    return copy_df
    
def testing():
    conn = sqlite3.connect('rosteriq.db')
    year, cluster_num, pos = 2021, 1, "G"
    standardized_player_df, scaler = get_standardized_player_rate_stats(conn, year, cluster_num, pos)
    median_vals_df = get_nPercentile_rate_stats_df(standardized_player_df, conn, year, cluster_num, pos)

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
