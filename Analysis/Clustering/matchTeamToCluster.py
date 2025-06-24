import json
import pandas as pd
import numpy as np

def match_team_to_cluster(team_stats,                          
                          scaling_path='/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Clustering/scaling_params.json',
                          profiles_path='/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Clustering/kclu_profiles.csv'):
    """
    Given:
      • team_stats: dict with keys
            ['team_adjoe','team_adjde','team_stltov_ratio',
             'team_oreb_per100','team_dreb_per100','team_eFG','team_P3M']
      • scaling_path: path to your scaling_params.json
      • profiles_path: path to your kclu_profiles.csv
    Returns:
      • cluster_id of the nearest centroid
      • DataFrame of all clusters sorted by distance
    """
    # 1) load scaling params
    with open(scaling_path, 'r') as f:
        params = json.load(f)
    centers = np.array(params['center'])
    scales  = np.array(params['scale'])

    # 2) load cluster centroids
    profiles = pd.read_csv(profiles_path, index_col=0)
    # X1..X7 must correspond to the same feature order as your scaling params
    featureX_names = []
    for i in range(1,len(centers) + 1):
        featureX_names.append("X" + str(i))
    centroids = profiles[featureX_names].values

    # 3) build raw vector in matching order
    feature_order = ['team_adjoe','team_adjde','team_stltov_ratio',
                     'team_oreb_per100','team_dreb_per100','team_eFG']
    raw_vec = np.array([ team_stats[f] for f in feature_order ])

    # 4) scale
    scaled_vec = (raw_vec - centers) / scales

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