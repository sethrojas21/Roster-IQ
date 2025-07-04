import json
import pandas as pd
import numpy as np

scaling_path = lambda year: f'/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Clustering/20ClusterData/{year}/scaling_params.json'
profiles_path = lambda year: f'/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Clustering/20ClusterData/{year}/kclu_profiles.csv'

def scale_center_vector_data(team_stats, year, profiles = None):
    if profiles is None:
        profiles = pd.read_csv(profiles_path(year), index_col=False)

    with open(scaling_path(year), 'r') as f:
        params = json.load(f)
    centers = np.array(params['center'])
    scales  = np.array(params['scale'])

    featureX_names = []
    for i in range(1,len(centers) + 1):
        featureX_names.append("X" + str(i))
    centroids = profiles[featureX_names].values    

    # 3) build raw vector in matching order
    feature_order = ['team_adjoe','team_adjde','team_stltov_ratio',
                     'team_oreb_per100','team_dreb_per100', 'team_threeRate', 'team_ftr', 'team_eFG']
    raw_vec = np.array([ team_stats[f] for f in feature_order ])

    # 4) scale
    scaled_vec = (raw_vec - centers) / scales

    return scaled_vec, centroids

def match_team_to_cluster(team_stats, year):
    profiles = pd.read_csv(profiles_path(year), index_col=False)

    scaled_vec, centroids = scale_center_vector_data(team_stats, year, profiles)    

    # 5) compute distances
    dists = np.linalg.norm(centroids - scaled_vec, axis=1)
    # find the index (position) of the closest centroid
    min_idx = int(np.argmin(dists))
    # use iloc on the 'ID' column to get the correct cluster_id
    nearest = int(profiles['ID'].iloc[min_idx])

    # 6) prepare sorted distances DF
    df = pd.DataFrame({
        'cluster_id': profiles['ID'],
        'distance': dists
    }).sort_values('distance').reset_index(drop=True)

    return nearest, df

def match_team_to_cluster_weights(team_stats, year, k = 3):
    _, df = match_team_to_cluster(team_stats, year)

    # Grab the k nearest clusters
    topK_df = df.head(k).copy()

    # ---- similarity transform ---------------------------------------------
    epsilon = 1e-6
    alpha = 1.5
    if 'alpha' not in locals() or alpha is None:
        print("Calculating alpha")
        # Heuristic: inverse of median distance to keep weights well‑behaved
        alpha = 1.0 / max(topK_df['distance'].median(), epsilon)

    # RBF kernel similarity
    sim = np.exp(-alpha * topK_df['distance'].values)

    # Normalise so that the weights sum to 1
    weights = sim / sim.sum()

    # Build and return dictionary
    return dict(zip(topK_df['cluster_id'].astype(int), weights))