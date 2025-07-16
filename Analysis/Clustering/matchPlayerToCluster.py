import pandas as pd
import numpy as np
from Clustering.pcaPlayers import project_to_pca

profiles_path = lambda year, pos : f"Analysis/Clustering/Players/{year}/KClustering/cluster_profiles_{pos}.csv"


def match_player_to_cluster(player_stats, year, pos):
    profiles = pd.read_csv(profiles_path(year, pos), index_col=False)

    pca_df = project_to_pca(player_stats, pos, year)
    
    # 5) compute distances
    # Extract centroid coordinates (assumes PC columns in profiles)
    pc_columns = [col for col in profiles.columns if col.startswith('PC')]
    centroids = profiles[pc_columns].values

    # Assuming player_stats is a single row, get its PCA values
    player_vec = pca_df.iloc[0].values

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


def match_player_to_cluster_weights(player_stats, year, pos, k=2, alpha=None, method='inverse_pow', power=1.5):
    _, df = match_player_to_cluster(player_stats, year, pos)
    

    # Grab the k nearest clusters
    topK_df = df.head(k).copy()
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

    # Build and return dictionary
    return dict(zip(topK_df['cluster_id'].astype(int), weights))