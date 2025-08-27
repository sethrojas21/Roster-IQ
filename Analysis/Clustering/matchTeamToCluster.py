import json
import pandas as pd
import numpy as np

scaling_path = lambda year: f'/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Clustering/Teams/{year}/PCA/params.json'
profiles_path = lambda year: f'/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Clustering/Teams/{year}/KClustering/profiles.csv'
rot_path = lambda year: f'Analysis/Clustering/Teams/{year}/PCA/rotation.json'

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

def project_to_pca(df_raw, year):
    """
    Project new data into an existing PCA space defined by an R prcomp object.
    Assumes df_raw is a pandas DataFrame of raw stats matching columns used to fit pca_model.
    pca_model should have attributes 'center', 'scale', and 'rotation' from prcomp.
    """
    # Load PCA parameters (center & scale) from JSON
    param_path = scaling_path(year)
    with open(param_path, 'r') as f:
        params = json.load(f)
    center = pd.Series(params['center'])
    scale  = pd.Series(params['scale'])

    # Load rotation matrix from JSON
    with open(rot_path(year), 'r') as f:
        rot_dict = json.load(f)
    # rot_dict is a mapping from feature name to PC loadings
    # Convert list of dicts into DataFrame, drop feature names
    rotation_df = pd.DataFrame(rot_dict)
    if 'feature' in rotation_df.columns:
        rotation_df = rotation_df.drop(columns=['feature'])
    # Ensure PC columns are in numeric order
    pc_cols = sorted([c for c in rotation_df.columns if c.startswith('PC')],
                     key=lambda x: int(x.replace('PC', '')))
    rotation = rotation_df[pc_cols].values
    
    # Reorder columns to match PCA feature order
    df = df_raw

    # Center and scale
    center.index = df.index
    scale.index = df.index
    df = df - center    
    df = df.div(scale)

    # Project into PCA space
    projected = np.dot(df.values, rotation)
    pc_names = [f'PC{i+1}' for i in range(rotation.shape[1])]
    ret = pd.Series(projected, index=pc_names)
    return ret

def get_centroid(year):
    profiles = pd.read_csv(profiles_path(year), index_col=False)    
    pc_columns = [col for col in profiles.columns if col.startswith('PC')]
    centroids = profiles[pc_columns]

    return centroids

def match_team_to_cluster(team_stats, year):
    profiles = pd.read_csv(profiles_path(year), index_col=False)
    
    proj_vec = project_to_pca(team_stats, year)
    pc_columns = [col for col in profiles.columns if col.startswith('PC')]
    centroids = profiles[pc_columns]

    # 5) compute distances
    dists = np.linalg.norm(centroids - proj_vec, axis=1)
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

def match_team_cluster_to_label(year, ids_or_id, rationale=False):
    """
    If ids_or_id is a list/tuple, returns a list of labels (or (label, rationale) tuples).
    Otherwise returns a single label (or tuple).
    """
    # load lookup once
    with open('Analysis/Clustering/Teams/archetypeLabels.json') as f:
        data = json.load(f)

    def _lookup(single_id):
        single_id = int(single_id)
        year_s, id_s = str(year), str(single_id)
        clu = data[year_s]["Teams"][id_s]
        return (clu['label'], clu['rationale']) if rationale else clu['label']

    if isinstance(ids_or_id, (list, tuple)):
        return [_lookup(i) for i in ids_or_id]
    else:
        return _lookup(ids_or_id)

def match_team_to_cluster_weights(team_stats, year, k = 1):
    team_stats_srs = pd.Series(team_stats)    
    _, df = match_team_to_cluster(team_stats_srs, year)
    
    # Grab the k nearest clusters
    labels = []    
    topK_df = df.head(k).copy()
    for _, team in topK_df.iterrows():
        id = team['cluster_id']
        # label = match_team_cluster_to_label(year, id)
        # labels.append(label)

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