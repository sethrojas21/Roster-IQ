import pandas as pd
import numpy as np
import json
from Analysis.Clustering.pcaPlayers import project_to_pca
from collections.abc import Iterable

profiles_path = lambda year, pos : f"Analysis/Clustering/Players/{year}/KClustering/cluster_profiles_{pos}.csv"

def get_player_stats(player_id, season_year, conn):
    """Returns a series"""
    player_features_query = """
    SELECT
        p.player_name,
        ps.position,
        ps.season_year,
        ps.ts_percent,
        ps.ast_percent,
        ps.oreb_percent,
        ps.dreb_percent,
        ps.tov_percent,
        ps.ft_percent,        
        ps.stl_percent,
        ps.blk_percent,
        ps.usg_percent,
        ps.ftr / 100 AS ftr,
        CASE WHEN ps.FGA != 0 THEN (ps.threeA / ps.FGA) ELSE 0.00001 END AS threeRate,
        -- CASE WHEN ps.FGA != 0 THEN (ps.ast_pg * ps.adj_gp) / ps.FGA ELSE 0.00001 END AS ast_fga,
        CASE WHEN ps.FGA != 0 THEN (ps.rimA / ps.FGA) ELSE 0.00001 END AS rimRate,
        CASE WHEN ps.FGA != 0 THEN (ps.midA / ps.FGA) ELSE 0.00001 END AS midRate
    FROM Player_Seasons ps
    JOIN Players p ON ps.player_id = p.player_id
    WHERE ps.player_id = ? and ps.season_year = ?
    """

    return pd.read_sql(player_features_query, conn, params=(player_id, season_year)).iloc[0]

def match_player_to_cluster(player_stats, year, pos):
    """Leave player stats raw. Standardizes and pcas them here"""
    profiles = pd.read_csv(profiles_path(year, pos), index_col=False)
    pca_df = project_to_pca(player_stats, pos, year)
    
    # 5) compute distances
    # Extract centroid coordinates (assumes PC columns in profiles)
    pc_columns = [col for col in profiles.columns if col.startswith('PC')]
    centroids = profiles[pc_columns].astype(float).values

    # Assuming player_stats is a single row, get its PCA values
    player_vec = pca_df.iloc[0].astype(float).values

    # Compute Euclidean distances between player and each centroid
    dists = np.linalg.norm(centroids - player_vec, axis=1)
    # find the index of the closest centroid
    min_idx = int(np.argmin(dists))
    # retrieve the cluster ID
    nearest = int(profiles['ID'].iloc[min_idx])

    # 6) prepare sorted distances DF
    df = pd.DataFrame({
        'cluster_id': profiles['ID'],
        'distance': dists
    }).sort_values('distance').reset_index(drop=True)

    return nearest, df

def match_player_cluster_to_label(year, pos, ids_or_id, rationale=False):
    """
    If ids_or_id is a list/tuple, returns a list of labels (or (label, rationale) tuples).
    Otherwise returns a single label (or tuple).
    """
    positions_dict = {
        "G": "Guards",
        "F": "Forwards",
        "C": "Centers"
    }
    year_s = str(year)
    pos_s  = positions_dict[pos]

    # load once
    with open('Analysis/Clustering/Players/archetypeLables.json', 'r') as f:
        data = json.load(f)

    def _lookup(single_id):
        clu = data[year_s][pos_s][str(single_id)]
        if rationale:
            return clu['label'], clu['rationale']
        return clu['label']

    # dispatch on iterable vs. scalar
    if isinstance(ids_or_id, Iterable) and not isinstance(ids_or_id, (str, bytes)):
        return [_lookup(i) for i in ids_or_id]
    else:
        return _lookup(ids_or_id)


def get_only_plyr_features(player_stats : pd.Series):
        META = {'player_name','position','season_year'}
        nmeta_player_stats = player_stats.copy()
        nmeta_player_stats = nmeta_player_stats.drop(index = META).astype(float)
        return nmeta_player_stats

def match_player_to_cluster_weights(player_stats, year, pos, k=2, alpha=None, method='inverse', power=1.5):
    nmeta_player_stats = get_only_plyr_features(player_stats)
    _, df = match_player_to_cluster(nmeta_player_stats, year, pos)
        
    # Grab the k nearest clusters
    if k == 1 and pos in ["C"]:
        k = 2

    # if k == 2 and ((df.iloc[1]['distance'] - df.iloc[0]['distance']) / df.iloc[1]['distance'] > 0.8):
    #     k = 1
    topK_df = df.head(k).copy() 

    # ---- similarity transform ---------------------------------------------
    epsilon = 1e-6
    distances = topK_df['distance'].values
    if method == 'rbf':
        # Determine alpha: use provided fixed alpha or adaptive heuristic
        if alpha is None:
            alpha = 1.0 / max(topK_df['distance'].median(), epsilon)
        sim = np.exp(-alpha * distances)
    elif method == 'inverse':
        # Simple inverse-distance weighting
        sim = 1.0 / (distances + epsilon)
    elif method == 'inverse_pow':
        # Inverse-distance to a specified power
        sim = 1.0 / (distances ** power + epsilon)
    else:
        raise ValueError(f"Unknown method: {method}")
    # Normalize so that the weights sum to 1
    weights = sim / sim.sum()
    # k = 1
    # print(weights)
    # for weight in weights:
    #     if weight < 0.75:
    #         break
    #     else:
    #         k += 1
    # topK_df = df.head(k).copy()
    # weights = weights[:k]
    # Build and return dictionary
    return dict(zip(topK_df['cluster_id'].astype(int), weights))