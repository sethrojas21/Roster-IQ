import numpy as np
import pandas as pd
from Analysis.Helpers.standardization import filter_cluster_players

def weighted_cluster_mean(
    df: pd.DataFrame,
    team_clusters: list[int],
    player_clusters: list[int],
    team_weights: list[float],
    player_weights: list[float],
    stat_cols: list[str]
) -> pd.Series:
    """
    Return a weighted mean vector of `stat_cols`.
    
    Parameters
    ----------
    df : DataFrame
        Must contain columns 'team_cluster' and 'Cluster' (aka player cluster) +
        the stat columns you want to average.
    team_clusters, player_clusters : lists of cluster IDs you want to mix.
    team_weights, player_weights  : same length as the corresponding cluster lists.
    stat_cols : list of column names to average.
    """
    # ---- 1. normalise weights so each list sums to 1 ------------------------
    t_w = np.array(team_weights, dtype=float)
    p_w = np.array(player_weights, dtype=float)
    t_w /= t_w.sum()
    p_w /= p_w.sum()

    # Look-up dictionaries for quick access
    t_w_map = dict(zip(team_clusters, t_w))
    p_w_map = dict(zip(player_clusters, p_w))


    # ---- 2.–4. accumulate weighted means ------------------------------------
    numer = np.zeros(len(stat_cols), dtype=float)
    denom = 0.0

    for t in team_clusters:
        for p in player_clusters:
            w = t_w_map[t] * p_w_map[p]               # combined weight
            subset = df[(df["team_cluster"] == t) &
                         (df["Cluster"] == p)]
            if subset.empty:
                continue                              # no rows in this combo
            numer += w * subset[stat_cols].mean().to_numpy()
            denom += w

    if denom == 0:
        raise ValueError("No players matched the supplied clusters.")

    return pd.Series(numer / denom, index=stat_cols)