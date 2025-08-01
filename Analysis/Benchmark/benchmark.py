import pandas as pd
from Analysis.Helpers.standardization import standardized_player_rate_stats, filter_cluster_players

column_shift = 5

def get_benchmark_stats(
        nPercentile_players_cluster_df: pd.DataFrame,
        cluster_weights: dict[int, float],
        percentile: float = 0.5,
        player_cluster_weights: dict[int, float] = None
) -> pd.DataFrame:
    if "team_cluster" not in nPercentile_players_cluster_df.columns:
        raise ValueError("Input df must include a 'cluster_num' column.")

    
    # Rate‑stat feature columns start after the first 4 metadata columns
    stat_cols = nPercentile_players_cluster_df.columns[column_shift:-2]
    

    # 1. Per‑cluster percentile rows (keep cluster_num as index)
    per_cluster = (
        nPercentile_players_cluster_df
        .groupby("team_cluster")[stat_cols]
        .mean()
    )

    # 2. Attach team cluster weights
    per_cluster["w_team"] = per_cluster.index.map(cluster_weights).fillna(0)

    # 2b. Attach player cluster weights (if provided)
    if player_cluster_weights is not None:
        per_cluster["w_player"] = per_cluster.index.map(player_cluster_weights).fillna(1)
    else:
        per_cluster["w_player"] = 1

    # 3. Combine weights multiplicatively then normalize
    per_cluster["w_combined"] = per_cluster["w_team"] * per_cluster["w_player"]
    # Avoid all-zero
    if per_cluster["w_combined"].sum() == 0:
        per_cluster["w_combined"] = per_cluster["w_team"]
    per_cluster["w_norm"] = per_cluster["w_combined"] / per_cluster["w_combined"].sum()

    # 4. Weighted blend using normalized weights
    blended_vec = (
        per_cluster[stat_cols]
        .multiply(per_cluster["w_norm"], axis=0)
        .sum()
        .to_frame()
        .T
    )

    # Ensure the result has the same column order
    blended_vec = blended_vec[stat_cols]

    return blended_vec

def get_benchmark_info(query, conn, year, cluster_weights, player_weights, pos : str, percentile = 0.5, normalized = True):
    df, scaler = standardized_player_rate_stats(query, conn, year, 
                                                list(cluster_weights.keys()), 
                                                list(player_weights.keys()),
                                                pos, normalized)
    filtered_df = filter_cluster_players(df)
    length = len(filtered_df)
    return (scaler, get_benchmark_stats(filtered_df, cluster_weights, percentile), length)