import numpy as np
import pandas as pd
import json

def project_to_pca(df_raw, role, year):
    """
    Project new data into an existing PCA space defined by an R prcomp object.
    Assumes df_raw is a pandas DataFrame of raw stats matching columns used to fit pca_model.
    pca_model should have attributes 'center', 'scale', and 'rotation' from prcomp.
    """
    # Load PCA parameters (center & scale) from JSON
    param_path = f"Analysis/Clustering/Players/{year}/PCA/pca_params_{role}.json"
    with open(param_path, 'r') as f:
        params = json.load(f)
    center = np.array(params['center'])
    scale  = np.array(params['scale'])

    # Load rotation matrix from JSON
    rot_path = f"Analysis/Clustering/Players/{year}/PCA/pca_rotation_{role}.json"
    with open(rot_path, 'r') as f:
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
    
    # Prepare data
    df = df_raw.copy()
    # center and scale
    df = df - center
    df = df.div(scale, axis=1)
    # Project into PCA space
    projected = np.dot(df.values, rotation)
    pc_names = [f'PC{i+1}' for i in range(rotation.shape[1])]
    return pd.DataFrame(projected, index=df.index, columns=pc_names)
