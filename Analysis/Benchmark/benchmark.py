"""
Benchmark Statistics Module

This module provides functionality for calculating weighted benchmark statistics
used in player transfer analysis. It combines cluster-based weighting with
statistical aggregation to create representative benchmark values for player
comparison and evaluation.
"""

import pandas as pd
from Analysis.Helpers.standardization import standardized_player_rate_stats, filter_cluster_players
from Analysis.Helpers.weightedMean import weighted_cluster_mean

# Number of columns to skip when selecting statistical columns (excludes metadata)
column_shift = 5


def get_benchmark_stats(
        nPercentile_players_cluster_df: pd.DataFrame,
        cluster_weights: dict[int, float],
        player_cluster_weights: dict[int, float]
) -> pd.DataFrame:
    """
    Calculate weighted benchmark statistics from clustered player data.
    
    This function takes player data grouped by clusters and computes weighted
    benchmark statistics by combining team cluster weights with optional player
    cluster weights. The result is a single row of representative statistics
    that can be used for player comparison.
    
    Args:
        nPercentile_players_cluster_df (pd.DataFrame): DataFrame containing player stats 
                                                      with cluster assignments
        cluster_weights (dict[int, float]): Weights for each team cluster ID
        percentile (float): Percentile for statistical aggregation (default: 0.5 for median)
        player_cluster_weights (dict[int, float], optional): Weights for player clusters
    
    Returns:
        pd.DataFrame: Single-row DataFrame containing weighted benchmark statistics
        
    Raises:
        ValueError: If the input DataFrame lacks required 'team_cluster' column
    """
    # Validate input DataFrame has required cluster column
    if "team_cluster" not in nPercentile_players_cluster_df.columns:
        raise ValueError("Input df must include a 'cluster_num' column.")

    # Extract statistical columns (skip metadata columns at start and cluster columns at end)
    # Rate-stat feature columns start after the first 5 metadata columns
    stat_cols = nPercentile_players_cluster_df.columns[column_shift:-2]

    # === STEP 1: Prepare clusters and weights ===
    team_clusters = list(cluster_weights.keys())
    team_weights = [cluster_weights[t] for t in team_clusters]
    if player_cluster_weights is not None:
        player_clusters = list(player_cluster_weights.keys())
        player_weights = [player_cluster_weights[p] for p in player_clusters]
    else:
        player_clusters = []
        player_weights = []

    # === STEP 2: Compute weighted means ===
    weighted_series = weighted_cluster_mean(
        nPercentile_players_cluster_df,
        team_clusters=team_clusters,
        player_clusters=player_clusters,
        team_weights=team_weights,
        player_weights=player_weights,
        stat_cols=stat_cols
    )

    # === STEP 3: Convert to single-row DataFrame ===
    blended_vec = weighted_series.to_frame().T
    blended_vec = blended_vec[stat_cols]

    return blended_vec

def get_benchmark_info(query, conn, year, cluster_weights, player_weights, pos : str, normalized = True):
    """
    Generate complete benchmark information including scaler and benchmark statistics.
    
    This is the main entry point for benchmark generation. It loads and standardizes
    player data from specified clusters, applies performance filters, and computes
    weighted benchmark statistics along with the fitted scaler for normalization.
    
    Args:
        query (str): SQL query fragment specifying which statistics to select
        conn (sqlite3.Connection): Database connection
        year (int): Season year for data retrieval
        cluster_weights (dict): Weights for each team cluster ID
        player_weights (dict): Weights for each player cluster ID  
        pos (str): Player position (e.g., 'PG', 'SG', 'SF', 'PF', 'C')
        percentile (float): Percentile for statistical aggregation (default: 0.5)
        normalized (bool): Whether to apply z-score normalization (default: True)
    
    Returns:
        tuple: (scaler, benchmark_stats, sample_size)
            - scaler (StandardScaler): Fitted scaler for data normalization
            - benchmark_stats (pd.DataFrame): Weighted benchmark statistics
            - sample_size (int): Number of players in the filtered dataset
    """
    # Load and standardize player data from specified clusters
    # This gets players matching both team and player cluster criteria
    df, scaler = standardized_player_rate_stats(query, conn, year, 
                                                list(cluster_weights.keys()),  # Team cluster IDs
                                                list(player_weights.keys()),   # Player cluster IDs
                                                pos, normalized)
    
    # Apply performance filters to remove underperforming players
    # This typically filters by team quality (Barthag ranking) and individual BPM
    filtered_df = filter_cluster_players(df)
    
    # Calculate sample size for effective sample size computations
    length = len(filtered_df)
    # Generate weighted benchmark statistics using cluster weights
    benchmark_stats = get_benchmark_stats(filtered_df, cluster_weights, player_weights)
    
    # Return complete benchmark package: scaler for normalization, 
    # benchmark values for comparison, and sample size for confidence
    return (scaler, benchmark_stats, length)