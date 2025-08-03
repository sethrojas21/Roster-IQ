"""
Benchmark Player Initialization Module

This module contains the InitBenchmarkPlayer class which initializes and manages
benchmark data for player replacement analysis. It handles clustering of both
team and player data, and provides methods for accessing different statistical
benchmarks used in transfer success evaluation.
"""

from Analysis.Helpers.dataLoader import get_incoming_synthetic_roster
from Analysis.SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df
from Analysis.Clustering.matchTeamToCluster import match_team_to_cluster_weights, match_team_cluster_to_label
from Analysis.Clustering.matchPlayerToCluster import get_player_stats, match_player_to_cluster_weights, match_player_cluster_to_label
from Analysis.Benchmark.benchmark import get_benchmark_info
import numpy as np


class InitBenchmarkPlayer:
    """
    Initialize and manage benchmark data for player replacement analysis.
    
    This class handles the initialization of both team and player clustering data,
    manages statistical queries for different benchmark types, and provides
    cached access to benchmark calculations.
    """
            
    def __init__(self, conn, team_name, incoming_season_year, player_id_to_replace):
        """
        Initialize benchmark player with team and player clustering data.
        
        Args:
            conn (sqlite3.Connection): Database connection
            team_name (str): Name of the team
            incoming_season_year (int): The upcoming season year
            player_id_to_replace (int): ID of the player being replaced
        """
        # Store metadata for benchmark calculations
        self.conn = conn
        self.team_name = team_name
        self.season_year = incoming_season_year
        self.replaced_plyr_id = player_id_to_replace
        
        # Clustering parameters - using k=1 for nearest cluster matching
        self.team_k = 1
        self.player_k = 2
        max_k = 2  # Maximum clusters to consider (not currently used)

        # === TEAM CLUSTERING SETUP ===
        # Get the synthetic roster (team without the replaced player)
        player_stats_df, _ = get_incoming_synthetic_roster(conn, team_name, incoming_season_year, player_id_to_replace)
        
        # Aggregate individual player stats to team-level statistics
        aggregated_team_stats = aggregate_team_stats_from_players_df(player_stats_df)
        
        # Match the team to cluster(s) and get weights based on similarity
        self.team_clusterID_weights_dict = match_team_to_cluster_weights(aggregated_team_stats,
                                                                   incoming_season_year,
                                                                   k=self.team_k)
        
        # Extract cluster IDs and their corresponding weights
        self.team_ids = list(self.team_clusterID_weights_dict.keys())
        self.team_weights = list(self.team_clusterID_weights_dict.values())
        
        # Team cluster labels (currently disabled/empty)
        # self.team_labels = match_team_cluster_to_label(incoming_season_year,
        #                                           self.team_ids)
        self.team_labels = []
        
        # === PLAYER CLUSTERING SETUP ===
        # Get statistics for the player being replaced
        self.replaced_plyr_stats = get_player_stats(player_id_to_replace, incoming_season_year, conn)    
        self.replaced_plyr_pos = self.replaced_plyr_stats['position']
        
        # Match the replaced player to cluster(s) and get weights
        self.plyr_clusterID_weights_dict = match_player_to_cluster_weights(self.replaced_plyr_stats,
                                                                      incoming_season_year,
                                                                      self.replaced_plyr_pos,
                                                                      k=self.player_k,
                                                                      adaptive_k=False,
                                                                      team_id=self.team_ids[0])
        
        # Extract player cluster IDs and weights
        self.plyr_ids = list(self.plyr_clusterID_weights_dict.keys())
        self.plyr_weights = list(self.plyr_clusterID_weights_dict.values())
        
        # Player cluster labels (currently disabled/empty)
        # self.plyr_labels = match_player_cluster_to_label(incoming_season_year,
        #                                                  self.replaced_plyr_pos,
        #                                                  self.plyr_ids)
        self.plyr_labels = []

        # === BENCHMARK CACHING ===
        # Cache dictionaries to store computed benchmarks (avoid recomputation)
        self.fs_benchmark_dict_saved = None   # Four Factors + Shot Selection benchmark
        self.vocbp_benchmark_dict_saved = None  # Value Over Collegiate Baseline Player benchmark
        self.length = 0  # Sample size for effective sample size calculations


    def fs_query():
        """
        SQL query fragment for Four Factors + Shot Selection statistics.
        
        This query selects advanced basketball statistics focused on:
        - Usage percentage: How much of team's possessions a player uses
        - Three-point rate: Percentage of shots taken from beyond the arc
        - Assist-to-FGA ratio: Playmaking relative to shot attempts
        - Field goal attempts per game: Volume of shot attempts
        - Free throw rate: Ability to get to the free throw line
        - Shot location rates: Distribution of shots by court area (rim, mid-range)
        
        Returns:
            str: SQL query fragment for FS (Four Factors + Shot Selection) statistics
        """
        return """
        -- ps.efg_percent,     # Effective field goal percentage (commented out)
        -- ps.ast_percent,     # Assist percentage (commented out)
        -- ps.oreb_percent,    # Offensive rebound percentage (commented out)
        -- ps.dreb_percent,    # Defensive rebound percentage (commented out)
        -- ps.tov_percent,     # Turnover percentage (commented out)
        -- ps.ft_percent,      # Free throw percentage (commented out)
        -- ps.stl_percent,     # Steal percentage (commented out)
        -- ps.blk_percent,     # Block percentage (commented out)
        ps.usg_percent,                                    -- Usage percentage
        (ps.threeA / ps.FGA) AS threeRate,                -- Three-point attempt rate
        (ps.ast_pg * ps.adj_gp) / ps.FGA AS ast_fga,      -- Assists per field goal attempt
        ps.FGA / ps.adj_gp AS fg_pg,                       -- Field goal attempts per game
        ps.ftr,                                            -- Free throw rate
        (ps.rimA / ps.FGA) AS rimRate,                     -- Rim shot attempt rate
        (ps.midA / ps.FGA) AS midRate                      -- Mid-range shot attempt rate
    """

    def vocbp_query():
        """
        SQL query fragment for Value Over Collegiate Baseline Player statistics.
        
        This query selects percentage-based statistics that measure a player's
        contribution relative to collegiate baseline performance:
        - Assist percentage: Share of team assists while on court
        - Rebounding percentages: Share of available rebounds
        - Free throw percentage: Shooting accuracy from the line
        - Defensive percentages: Steals and blocks relative to opportunities
        - True shooting percentage: Overall shooting efficiency
        
        Returns:
            str: SQL query fragment for VOCBP statistics
        """
        return """
        -- (ps.AST / ps.POSS) * 100 AS ast100,      # Assists per 100 possessions (commented)
        -- (ps.OREB / ps.POSS) * 100 AS oreb100,    # Off rebounds per 100 poss (commented)
        -- (ps.DREB / ps.POSS) * 100 AS dreb100,    # Def rebounds per 100 poss (commented)
        -- (CAST(ps.STL AS REAL) * 100 / ps.POSS) AS stl100,  # Steals per 100 poss (commented)
        --(CAST(ps.BLK AS REAL) * 100 / ps.POSS) AS blk100,   # Blocks per 100 poss (commented)
        -- ps.tov_percent,                           # Turnover percentage (commented)
        ps.ast_percent,          -- Percentage of team assists while on court
        ps.oreb_percent,         -- Percentage of offensive rebounds grabbed
        ps.dreb_percent,         -- Percentage of defensive rebounds grabbed
        ps.ft_percent,           -- Free throw shooting percentage
        ps.stl_percent,          -- Percentage of opponent possessions ending in steal
        ps.blk_percent,          -- Percentage of opponent 2PA blocked
        ps.ts_percent            -- True shooting percentage (overall efficiency)
    """

    def effective_sample_size(weights, lengths):
        """
        Calculate the effective sample size for weighted cluster analysis.
        
        This method computes how many "effective" observations we have when
        combining multiple clusters with different weights and sizes. It accounts
        for the fact that higher weights and larger clusters contribute more to
        the overall sample.
        
        Args:
            weights (list): Weight of each cluster in the analysis
            lengths (list): Number of players in each cluster
            
        Returns:
            float: Effective sample size accounting for cluster weights and sizes
        """
        # Distribute cluster weight equally among all players in that cluster
        expanded_weights = []
        for w, l in zip(weights, lengths):
            if l > 0:  # Only process clusters with players
                # Each player gets equal share of cluster's total weight
                expanded_weights.extend([w / l] * l)
        
        expanded_weights = np.array(expanded_weights)
        
        # Handle edge case of no weights
        if expanded_weights.sum() == 0:
            return 0
        
        # Calculate effective sample size using the formula:
        # ESS = (sum of weights)² / (sum of squared weights)
        # This gives the equivalent number of equally-weighted observations
        return (expanded_weights.sum() ** 2) / (expanded_weights ** 2).sum()
        

    def fs_benchmark(self, adaptive_k = True):
        """
        Get or compute Four Factors + Shot Selection benchmark data.
        
        This method retrieves cached FS benchmark data or computes it if not available.
        The benchmark includes both the scaler (for standardization) and benchmark values
        (median/representative values for the player's cluster context).
        
        Args:
            adaptive_k (bool): Whether to use adaptive k-means (currently unused)
            
        Returns:
            dict: Dictionary containing 'scalar' (StandardScaler) and 'vals' (benchmark values)
        """
        # Return cached data if available to avoid recomputation
        if self.fs_benchmark_dict_saved:
            return self.fs_benchmark_dict_saved
        
        # Compute benchmark info using FS query and cluster data
        scalar, bmark_vals, lth = get_benchmark_info(InitBenchmarkPlayer.fs_query(), 
                                                             self.conn, 
                                                             self.season_year,                                                              
                                                             self.team_clusterID_weights_dict,
                                                             self.plyr_clusterID_weights_dict,
                                                             self.replaced_plyr_pos)

        # Package results into dictionary
        dict = {
            "scalar" : scalar,    # StandardScaler fitted to benchmark data
            "vals" : bmark_vals   # Benchmark values (medians/representatives)
        }
        
        # Cache results and update sample size
        self.fs_benchmark_dict_saved = dict
        self.length = lth
        return dict
    
    def fs_scalar(self):
        """
        Get the StandardScaler for Four Factors + Shot Selection statistics.
        
        Returns:
            StandardScaler: Fitted scaler for FS statistics normalization
        """
        if self.fs_benchmark_dict_saved:
            return self.fs_benchmark_dict_saved['scalar']
        
        return self.fs_benchmark()['scalar']
        
    def fs_bmark_vals(self):
        """
        Get the benchmark values for Four Factors + Shot Selection statistics.
        
        Returns:
            np.ndarray: Representative/median values for FS statistics in player's context
        """
        if self.fs_benchmark_dict_saved:
            return self.fs_benchmark_dict_saved['vals']
        
        return self.fs_benchmark()['vals']

    def vocbp_benchmark(self):
        """
        Get or compute Value Over Collegiate Baseline Player benchmark data.
        
        This method retrieves cached VOCBP benchmark data or computes it if not available.
        VOCBP focuses on percentage-based statistics that measure player value relative
        to a baseline collegiate player.
        
        Returns:
            dict: Dictionary containing 'scalar' (StandardScaler) and 'vals' (benchmark values)
        """
        # Return cached data if available
        if self.vocbp_benchmark_dict_saved:
            return self.vocbp_benchmark_dict_saved

        # Compute benchmark info using VOCBP query and cluster data
        scalar, bmark_vals, lth = get_benchmark_info(InitBenchmarkPlayer.vocbp_query(), 
                                                             self.conn, 
                                                             self.season_year,                                                              
                                                             self.team_clusterID_weights_dict,
                                                             self.plyr_clusterID_weights_dict,
                                                             self.replaced_plyr_pos)

        # Package results into dictionary
        dict = {
            "scalar" : scalar,    # StandardScaler fitted to benchmark data
            "vals" : bmark_vals   # Benchmark values (medians/representatives)
        }

        # Cache results and update sample size
        self.vocbp_benchmark_dict_saved = dict
        self.length = lth
        return dict
    
    def vocbp_scalar(self):
        """
        Get the StandardScaler for Value Over Collegiate Baseline Player statistics.
        
        Returns:
            StandardScaler: Fitted scaler for VOCBP statistics normalization
        """
        if self.vocbp_benchmark_dict_saved:
            return self.vocbp_benchmark_dict_saved['scalar']
        
        return self.vocbp_benchmark()['scalar']
        
    def vocbp_bmark_vals(self):
        """
        Get the benchmark values for Value Over Collegiate Baseline Player statistics.
        
        Returns:
            np.ndarray: Representative/median values for VOCBP statistics in player's context
        """
        if self.vocbp_benchmark_dict_saved:
            return self.vocbp_benchmark_dict_saved['vals']
        
        return self.vocbp_benchmark()['vals']
