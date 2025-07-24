import pandas as pd
import numpy as np
from Clustering.pcaPlayers import project_to_pca
import json

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
        ps.usg_percent AS usg_rate,
        ps.ftr / 100 AS ftr,
        CASE WHEN ps.FGA != 0 THEN (ps.threeA / ps.FGA) ELSE 0.00001 END AS threeRate,
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

def match_player_cluster_to_label(year, pos, id, rationale = False):
    positions_dict = {
        "G" : "Guards",
        "F" : "Forwards",
        "C": "Centers"
    }

    with open('Analysis/Clustering/Players/archetypeLables.json', 'r') as file:
        data = json.load(file)
    
    year = str(year)
    pos = positions_dict[pos]
    id = str(id)
    clu = data[year][pos][id]
    if rationale:
        return (clu['label'], clu['rationale'])
    else:
        return clu['label']


def match_player_to_cluster_weights(player_stats, year, pos, k=1, alpha=None, method='inverse_pow', power=3):
    _, df = match_player_to_cluster(player_stats, year, pos)
        

    # Grab the k nearest clusters
    if k == 1 and pos == "C":
        k = 2

    topK_df = df.head(k).copy()
    for _, player in topK_df.iterrows():
        id = int(player['cluster_id'])
        print("Label:", match_player_cluster_to_label(year, pos, id))

    print(pos)  
    # print(topK_df)
    # print(topK_df.iloc[0]['distance'] / topK_df.iloc[1]['distance'])

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
    print("Player Weights", weights)
    # Build and return dictionary
    return dict(zip(topK_df['cluster_id'].astype(int), weights))