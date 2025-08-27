"""
Benchmark Statistics Module

This module provides functionality for calculating weighted benchmark statistics
used in player transfer analysis. It combines cluster-based weighting with
statistical aggregation to create representative benchmark values for player
comparison and evaluation.
"""

import pandas as pd
from Analysis.Helpers.standardization import standardized_player_rate_stats
from Analysis.Helpers.weightedMean import weighted_cluster_mean
from Analysis.config import Config
import numpy as np

# Number of columns to skip when selecting statistical columns (excludes metadata)

def get_benchmark_stats(
        benchmark_players_cluster_df: pd.DataFrame,
        cluster_weights: dict[int, float],
        player_cluster_weights: dict[int, float],
        debug: bool = False
) -> pd.Series:
    """
    Calculate weighted benchmark statistics from clustered player data.
    
    This function takes player data grouped by clusters and computes weighted
    benchmark statistics by combining team cluster weights with optional player
    cluster weights. The result is a single row of representative statistics
    that can be used for player comparison. No adjustment factors are applied in this function.
    
    Args:
        benchmark_players_cluster_df (pd.DataFrame): DataFrame containing player stats 
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
    if "team_cluster" not in benchmark_players_cluster_df.columns:
        raise ValueError("Input df must include a 'team_cluster' column.")
    if "Cluster" not in benchmark_players_cluster_df.columns:
        raise ValueError("Input df must include a 'Cluster' column.")

    # No adjustment factors are applied; use original data directly
    adjusted_df = benchmark_players_cluster_df
    # Extract statistical columns (skip metadata columns at start and cluster columns at end)
    # Rate-stat feature columns start after the first 5 metadata columns
    stat_cols = [col for col in adjusted_df.columns if col not in Config.NON_STAT_COLS]

    if debug:
        print(adjusted_df[stat_cols + ["player_name"]])

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
        adjusted_df,  # Use adjusted DataFrame instead of original
        team_clusters=team_clusters,
        player_clusters=player_clusters,
        team_weights=team_weights,
        player_weights=player_weights,
        stat_cols=stat_cols
    )

    # === STEP 3: Convert to single-row DataFrame ===
    blended_vec = weighted_series
    blended_vec = blended_vec[stat_cols]

    return blended_vec

def get_benchmark_info(query, conn, year, team_cluster_weights, player_cluster_weights, pos : str, normalized = True):
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
    team_cluster_ids = list(team_cluster_weights.keys())
    player_cluster_ids = list(player_cluster_weights.keys())

    df, scaler = standardized_player_rate_stats(query, conn, year, 
                                                team_cluster_ids,  # Team cluster IDs
                                                player_cluster_ids,   # Player cluster IDs
                                                pos, normalized)
     
     
# --- Compute individual-level ESS using Kish formula ---
    # Calculate individual weight for each player
    individual_weights = []
    for _, row in df.iterrows():
        w_team = team_cluster_weights.get(int(row["team_cluster"]), 0.0)
        w_player = player_cluster_weights.get(int(row["Cluster"]), 0.0)
        individual_weight = w_team * w_player
        individual_weights.append(individual_weight)
    
    individual_weights = np.array(individual_weights)
    
    if len(individual_weights) == 0 or individual_weights.sum() == 0:
        ess = 0.0
    else:
        # Normalize weights to sum to sample size (standard practice for ESS)
        n = len(individual_weights)
        individual_weights = individual_weights * n / individual_weights.sum()
        
        # Kish ESS formula: ESS = (Σ w_i)² / Σ w_i²
        sum_weights = individual_weights.sum()
        sum_weights_squared = (individual_weights ** 2).sum()
        ess = float((sum_weights ** 2) / sum_weights_squared)

    # Raw sample size (just len of filtered df)
    raw_n = int(len(df))

    # Generate weighted benchmark statistics using cluster weights
    benchmark_stats = get_benchmark_stats(df, 
                                          team_cluster_weights, 
                                          player_cluster_weights)
    
    # Return complete benchmark package: scaler for normalization, 
    # benchmark values for comparison, and sample size for confidence
    return (scaler, benchmark_stats, ess)