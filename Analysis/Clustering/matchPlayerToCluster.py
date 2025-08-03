"""
Player Clustering and Matching Module

This module provides functionality for matching individual players to predefined player clusters
based on their statistical profiles. It uses Principal Component Analysis (PCA) and K-means
clustering to categorize players into archetypes (e.g., "Floor General", "3&D Wing", "Rim Protector").

The clustering process involves:
1. Loading player statistics from database
2. Applying PCA dimensionality reduction to player features
3. Computing distances to cluster centroids in PCA space
4. Assigning players to nearest clusters with optional weighted similarity

Key Features:
- Player-to-cluster matching using Euclidean distance in PCA space
- Weighted cluster assignment using multiple similarity methods (RBF, inverse distance)
- Archetype label mapping for interpretable cluster descriptions
- Position-specific clustering (Guards, Forwards, Centers)
- Team context consideration for sample size adjustments

Basketball Statistics Used:
- Shooting: TS%, 3-point rate, rim rate, mid-range rate, FTr
- Playmaking: AST%, TOV%
- Rebounding: OREB%, DREB%
- Defense: STL%, BLK%
- Usage: USG%
"""

import pandas as pd
import numpy as np
import json
from Analysis.Clustering.pcaPlayers import project_to_pca
from collections.abc import Iterable
from Analysis.Clustering.labelArchetypes import get_sample_length_plyr_team_archeytpe
from Analysis.config import Config

# Lambda function to generate file paths for cluster profile CSVs by year and position
profiles_path = lambda year, pos : f"Analysis/Clustering/Players/{year}/KClustering/cluster_profiles_{pos}.csv"

def get_player_stats(player_id, season_year, conn):
    """
    Retrieve comprehensive statistical profile for a specific player in a given season.
    
    Extracts key basketball performance metrics from the database, including shooting efficiency,
    playmaking, rebounding, defense, and usage statistics. These features form the basis for
    player clustering and similarity analysis.
    
    Args:
        player_id (int): Unique identifier for the player
        season_year (int): Season year (e.g., 2024 for 2023-24 season) 
        conn: Database connection object
        
    Returns:
        pd.Series: Player's statistical profile containing:
            - Basic info: player_name, position, season_year
            - Shooting: ts_percent, threeRate, rimRate, midRate, ftr
            - Playmaking: ast_percent, tov_percent
            - Rebounding: oreb_percent, dreb_percent
            - Defense: stl_percent, blk_percent
            - Usage: usg_percent, ft_percent
            
    Note:
        Uses small epsilon values (0.00001) to avoid division by zero for shooting rates
        when players have 0 field goal attempts.
    """
    # SQL query to extract comprehensive player statistics
    # Joins Player_Seasons with Players table to get name and calculated rate stats
    player_features_query = """
    SELECT
        p.player_name,
        ps.position,
        ps.season_year,
        ps.ts_percent,                    -- True Shooting Percentage (shooting efficiency)
        ps.ast_percent,                   -- Assist Percentage (playmaking rate)
        ps.oreb_percent,                  -- Offensive Rebound Percentage
        ps.dreb_percent,                  -- Defensive Rebound Percentage  
        ps.tov_percent,                   -- Turnover Percentage
        ps.ft_percent,                    -- Free Throw Percentage
        ps.stl_percent,                   -- Steal Percentage (defensive activity)
        ps.blk_percent,                   -- Block Percentage (rim protection)
        ps.usg_percent,                   -- Usage Percentage (offensive involvement)
        ps.ftr / 100 AS ftr,             -- Free Throw Rate (normalized to 0-1 scale)
        -- Shot selection metrics (proportion of FGA from each zone)
        CASE WHEN ps.FGA != 0 THEN (ps.threeA / ps.FGA) ELSE 0.00001 END AS threeRate,
        -- CASE WHEN ps.FGA != 0 THEN (ps.ast_pg * ps.adj_gp) / ps.FGA ELSE 0.00001 END AS ast_fga,
        CASE WHEN ps.FGA != 0 THEN (ps.rimA / ps.FGA) ELSE 0.00001 END AS rimRate,
        CASE WHEN ps.FGA != 0 THEN (ps.midA / ps.FGA) ELSE 0.00001 END AS midRate
    FROM Player_Seasons ps
    JOIN Players p ON ps.player_id = p.player_id
    WHERE ps.player_id = ? and ps.season_year = ?
    """

    return pd.read_sql(player_features_query, conn, params=(player_id, season_year)).iloc[0]

def match_player_to_cluster(player_stats, year, pos):
    """
    Match a player to their closest cluster based on statistical similarity in PCA space.
    
    This function performs the core clustering assignment by:
    1. Loading pre-computed cluster centroids for the given year and position
    2. Projecting player stats into PCA space using position-specific transformations
    3. Computing Euclidean distances to all cluster centroids
    4. Returning the nearest cluster ID and full distance ranking
    
    Args:
        player_stats (pd.Series): Raw player statistics (will be standardized internally)
        year (int): Season year for cluster model selection
        pos (str): Player position ("G", "F", or "C")
        
    Returns:
        tuple: (nearest_cluster_id, distance_dataframe)
            - nearest_cluster_id (int): ID of the closest matching cluster
            - distance_dataframe (pd.DataFrame): All clusters ranked by distance
                with columns ['cluster_id', 'distance']
                
    Note:
        Player stats are standardized and PCA-transformed within this function.
        The clustering model must already exist for the specified year/position.
    """
    # Load pre-computed cluster centroids for this year and position
    profiles = pd.read_csv(profiles_path(year, pos), index_col=False)
    
    # Transform player stats into PCA space using existing model
    pca_df = project_to_pca(player_stats, pos, year)
    
    # Extract centroid coordinates from cluster profiles
    # Assumes columns named 'PC1', 'PC2', etc. for principal components
    pc_columns = [col for col in profiles.columns if col.startswith('PC')]
    centroids = profiles[pc_columns].astype(float).values

    # Get player's PCA coordinates (single row becomes vector)
    player_vec = pca_df.iloc[0].astype(float).values

    # Compute Euclidean distances between player and each cluster centroid
    dists = np.linalg.norm(centroids - player_vec, axis=1)
    
    # Find the index of the closest centroid
    min_idx = int(np.argmin(dists))
    
    # Retrieve the cluster ID corresponding to the nearest centroid
    nearest = int(profiles['ID'].iloc[min_idx])

    # Create a DataFrame with all clusters ranked by distance for analysis
    df = pd.DataFrame({
        'cluster_id': profiles['ID'],
        'distance': dists
    }).sort_values('distance').reset_index(drop=True)

    return nearest, df

def match_player_cluster_to_label(year, pos, ids_or_id, rationale=False):
    """
    Convert cluster IDs to human-readable archetype labels.
    
    This function maps numeric cluster IDs to interpretable basketball archetypes
    (e.g., "Floor General", "3&D Wing", "Rim Protector") using pre-generated labels.
    Supports both single cluster lookup and batch processing of multiple clusters.
    
    Args:
        year (int): Season year for archetype mapping
        pos (str): Player position ("G", "F", "C")
        ids_or_id (int or list): Single cluster ID or list of cluster IDs
        rationale (bool): If True, returns (label, rationale) tuples with explanations
        
    Returns:
        str or list: Archetype label(s) for the cluster(s)
            - Single string if ids_or_id is scalar
            - List of strings if ids_or_id is iterable
            - If rationale=True, returns tuples with explanations
            
    Example:
        match_player_cluster_to_label(2024, "G", 3) -> "Floor General"
        match_player_cluster_to_label(2024, "F", [1,2], True) -> 
            [("3&D Wing", "High 3P%, good defense"), ("Stretch 4", "Floor spacing")]
    """
    # Map position abbreviations to full names used in JSON structure
    positions_dict = Config.POSITION_DICT
    year_s = str(year)
    pos_s  = positions_dict[pos]

    # Load archetype labels from JSON file (contains all years/positions/clusters)
    with open('Analysis/Clustering/Players/archetypeLables.json', 'r') as f:
        data = json.load(f)

    def _lookup(single_id):
        """Helper function to lookup a single cluster ID"""
        clu = data[year_s][pos_s][str(single_id)]
        if rationale:
            return clu['label'], clu['rationale']
        return clu['label']

    # Handle both iterable and scalar inputs
    # Return list for iterable inputs, single value for scalar inputs
    if isinstance(ids_or_id, Iterable) and not isinstance(ids_or_id, (str, bytes)):
        return [_lookup(i) for i in ids_or_id]
    else:
        return _lookup(ids_or_id)


def get_only_plyr_features(player_stats : pd.Series):
    """
    Extract only numerical statistical features from player data.
    
    Removes metadata columns (player_name, position, season_year) and converts
    remaining basketball statistics to float values for clustering analysis.
    
    Args:
        player_stats (pd.Series): Complete player statistical profile including metadata
        
    Returns:
        pd.Series: Numerical features only, converted to float type
            - Removes: player_name, position, season_year
            - Keeps: all rate stats, percentages, and derived metrics
            
    Note:
        This function prepares data for PCA transformation and clustering by ensuring
        only numerical features are included and properly typed.
    """
    # Define metadata columns that should be excluded from clustering
    META = {'player_name','position','season_year'}
    
    # Create copy and remove metadata columns
    nmeta_player_stats = player_stats.copy()
    nmeta_player_stats = nmeta_player_stats.drop(index = META).astype(float)
    
    return nmeta_player_stats



def match_player_to_cluster_weights(player_stats, 
                                    year, 
                                    pos,
                                    team_id,
                                    adaptive_k = True,
                                    k=2, 
                                    alpha=None, 
                                    method='inverse_pow', 
                                    power=1.5):
    """
    Assign weighted similarity scores to the k nearest player clusters.
    
    Instead of hard assignment to a single cluster, this function computes similarity
    weights for the k nearest clusters. This provides more nuanced player representation
    and accounts for players who may exhibit characteristics of multiple archetypes.
    
    Args:
        player_stats (pd.Series): Complete player statistical profile (with metadata)
        year (int): Season year for cluster model selection
        pos (str): Player position ("G", "F", "C")
        team_id (int, optional): Team ID for sample size adjustment
        k (int): Number of nearest clusters to consider (default: 2)
        alpha (float, optional): RBF kernel parameter (auto-calculated if None)
        method (str): Similarity calculation method
            - 'rbf': Radial Basis Function (Gaussian) kernel
            - 'inverse': Simple inverse distance weighting
            - 'inverse_pow': Inverse distance to specified power
        power (float): Power parameter for 'inverse_pow' method (default: 1.5)
        
    Returns:
        dict: Mapping of cluster_id -> weight for k nearest clusters
            - Weights sum to 1.0
            - Higher weights indicate stronger similarity
            
    Example:
        weights = match_player_to_cluster_weights(stats, 2024, "G", k=3)
        # Returns: {2: 0.6, 5: 0.3, 1: 0.1}
        # Player is 60% similar to cluster 2, 30% to cluster 5, 10% to cluster 1
    """
    # Extract only numerical features for clustering
    nmeta_player_stats = get_only_plyr_features(player_stats)
    
    # Get distance ranking to all clusters
    _, df = match_player_to_cluster(nmeta_player_stats, year, pos)

    # Get the top cluster for potential sample size adjustment
    top_plyr_cluster_id = df.iloc[0]['cluster_id']

    # Adjust k based on team-specific sample size if team_id provided
    if adaptive_k and team_id is not None:
        k = 1
        # Check if there's sufficient data for this player type on this team
        length = get_sample_length_plyr_team_archeytpe(top_plyr_cluster_id,
                                                       team_id,
                                                       year,
                                                       pos)
        # Use fewer clusters if sample size is small to avoid overfitting
        if length <= Config.ESS_THRESHOLD:
            k = 2
        
    # Special handling for centers (typically fewer distinct archetypes)
    if pos == ["C"]:
        k = 2

    # Select the k nearest clusters for weighted assignment
    topK_df = df.head(k).copy() 

    # Transform distances to similarity weights using specified method
    epsilon = 1e-6  # Small value to prevent division by zero
    distances = topK_df['distance'].values
    
    if method == 'rbf':
        # Radial Basis Function (Gaussian) kernel: sim = exp(-alpha * distance)
        # Alpha controls kernel width - larger alpha = more localized similarity
        if alpha is None:
            # Auto-calculate alpha based on median distance
            alpha = 1.0 / max(topK_df['distance'].median(), epsilon)
        sim = np.exp(-alpha * distances)
        
    elif method == 'inverse':
        # Simple inverse distance weighting: sim = 1 / (distance + epsilon)
        sim = 1.0 / (distances + epsilon)
        
    elif method == 'inverse_pow':
        # Inverse distance to a power: sim = 1 / (distance^power + epsilon)
        # Higher power values make similarity more localized
        sim = 1.0 / (distances ** power + epsilon)
        
    else:
        raise ValueError(f"Unknown method: {method}")
    
    # Normalize similarities so they sum to 1.0 (probability distribution)
    weights = sim / sim.sum()

    # Return dictionary mapping cluster IDs to their similarity weights
    return dict(zip(topK_df['cluster_id'].astype(int), weights))