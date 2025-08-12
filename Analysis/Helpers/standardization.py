import pandas as pd
from sklearn.preprocessing import StandardScaler
from Analysis.Helpers.dataLoader import load_players_from_multiple_clusters
from Analysis.config import Config
import numpy as np

def standardized_player_rate_stats(stat_query, conn, year, team_cluster_nums, player_cluster_nums, pos : str, normalized = True):
    """
    New function that deals with weights of multiple clusters
    """
    rate_stats_df = load_players_from_multiple_clusters(stat_query, 
                                                        conn, 
                                                        year, 
                                                        team_cluster_nums, 
                                                        player_cluster_nums,
                                                        pos)    
    
    # Exclude meta data and non-stat stats
    columns = [col for col in rate_stats_df.columns if col not in Config.NON_STAT_COLS]

    df = rate_stats_df.copy()
    
    # scale only the rate-stat columns if requested
    scaler = None
    if normalized:
        scaler = StandardScaler().fit(df[columns])
        scaled_vals = scaler.transform(df[columns])                
        # overwrite only the rate columns with their scaled versions                   
        df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    
    return (df, scaler)

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

def filter_cluster_players(df, winningTeams=False, bpm=True): 
    copy_df = df.copy()
    
    if winningTeams:       
        threshold = copy_df['barthag_rank'].quantile(0.5)
        copy_df = copy_df[copy_df['barthag_rank'] <= threshold]  

    if bpm:
        copy_df = copy_df[copy_df['bpm'] > Config.BPM_REPLACEMENT]
    
    return copy_df

# Helper to create a subset StandardScaler from a fitted scaler
def _subset_standard_scaler(fitted_scaler: StandardScaler, full_order: list, keep_cols: list) -> StandardScaler:
    """Build a new StandardScaler restricted to keep_cols, preserving fitted params.
    full_order is the column order the original scaler was fit on.
    """
    # map keep_cols to indices in the original fit order
    pos = {c: i for i, c in enumerate(full_order)}
    idx = [pos[c] for c in keep_cols]

    new_scaler = StandardScaler(copy=True, with_mean=True, with_std=True)
    # copy fitted attributes for the subset
    new_scaler.mean_ = fitted_scaler.mean_[idx]
    if hasattr(fitted_scaler, "var_"):
        new_scaler.var_ = fitted_scaler.var_[idx]
    new_scaler.scale_ = fitted_scaler.scale_[idx]
    new_scaler.n_features_in_ = len(idx)
    try:
        
        new_scaler.feature_names_in_ = np.array(keep_cols, dtype=object)
    except Exception:
        pass
    if hasattr(fitted_scaler, "n_samples_seen_"):
        new_scaler.n_samples_seen_ = fitted_scaler.n_samples_seen_
    return new_scaler