from typing import List
import pandas as pd
import numpy as np
from Analysis.Helpers.weightedMean import weighted_cluster_mean
from Analysis.CalculateScores.adjustmentFactor import apply_adj_fact_to_plyr_srs

def successful_transfer(plyr_clusters: list, 
                        team_clusters: list, 
                        plyr_stats: pd.Series, 
                        all_plyr_stats: pd.DataFrame, 
                        plyr_weights: list, 
                        team_weights: list,
                        off_factor: float = None,
                        def_factor: float = None,
                        debug: bool = False):
    """
    plyr_stats -> should have players name and the players stats from the current year (the year after playing)
    all_plyr_stats -> data frame of all players stats from the previous season of current plyr_stats 
        and the team and player clusters they were mapped to
    """

    if off_factor and def_factor:
        plyr_stats = apply_adj_fact_to_plyr_srs(plyr_stats, off_factor, def_factor)

    META = ["player_name", "team_name", "season_year", "player_id", "position"]

    # ---- 1. identify numeric stat columns ----------------------------------
    stats_columns = [
        c for c in all_plyr_stats.columns
        if c not in META + ["team_cluster", "Cluster"]
    ]

    print(f"Stats columns: {stats_columns}")

    # ---- 2. weighted mean via helper ---------------------------------------

    mean_vals = weighted_cluster_mean(
        all_plyr_stats,
        team_clusters=team_clusters,
        player_clusters=plyr_clusters,
        team_weights=team_weights,
        player_weights=plyr_weights,
        stat_cols=stats_columns,
    )

    # ---- 3. build weighted dataframe once for std & ESS --------------------
    combined_df = pd.DataFrame()
    for p_cluster, t_cluster, p_weight, t_weight in zip(
            plyr_clusters, team_clusters, plyr_weights, team_weights):
        subset = all_plyr_stats[
            (all_plyr_stats["team_cluster"] == t_cluster) &
            (all_plyr_stats["Cluster"] == p_cluster)
        ]
        if subset.empty:
            continue
        weighted_subset = subset.copy()
        weighted_subset["weight"] = p_weight * t_weight
        combined_df = pd.concat([combined_df, weighted_subset], ignore_index=True)

    if combined_df.empty:
        return (0.0, False)

    # ---- 4. weighted std for each stat -------------------------------------
    std_vals = pd.Series(index=stats_columns, dtype=float)
    for col in stats_columns:
        vals = combined_df[col].dropna()
        wts = combined_df.loc[vals.index, "weight"]
        if vals.empty:
            std_vals[col] = 1  # fallback when no data
        else:
            std = np.sqrt(np.average((vals - mean_vals[col]) ** 2, weights=wts))
            std_vals[col] = std if std != 0 else 1  # avoid divide‑by‑zero

    # Player’s value vector to score
    plyr_vals = plyr_stats[stats_columns]

    # --- Effective Sample Size (ESS) check ---
    def effective_sample_size(weights):
        weights = np.array(weights)
        if weights.sum() == 0:
            return 0
        return (weights.sum()**2) / (weights**2).sum()

    ess = effective_sample_size(combined_df['weight'])

    # Define impact weights for key stats
    impact_weights = {
        'dporpag': 1.2,
        'porpag': 1.2,
        'ts_percent': 1.1
    }

    # Compute weighted z-score sum
    flipped_stats_lst = ['tov_percent', 'adjde', 'dporpag']
    score_sum = 0.0
    weight_sum = 0.0
    for col in stats_columns:
        # raw z-score relative to mean
        z = (plyr_vals[col] - mean_vals[col]) / std_vals[col]
        # invert for metrics where lower is better
        if col in flipped_stats_lst:
            z = -z
        weight = impact_weights.get(col, 1.0)
        score_sum += z * weight
        weight_sum += weight

    # Normalize to final score
    score = score_sum / weight_sum

    # Debug: log large deviations
    if debug:
        for col in stats_columns:
            dev = (plyr_vals[col] - mean_vals[col]) / std_vals[col]
            if col in flipped_stats_lst:
                dev *= -1
            if abs(dev) >= 0.75:
                print(f"{col} deviation: {dev:.2f} SDs from mean")

    THRESHOLD = -0.05
    is_successful = score > THRESHOLD
    return (score, is_successful, ess)
    


def testing():
    pass  # Placeholder for testing logic

if __name__ == '__main__':
    testing()