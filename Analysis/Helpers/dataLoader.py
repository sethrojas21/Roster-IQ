import pandas as pd
from Analysis.Clustering.matchTeamToCluster import project_to_pca, get_centroid
import numpy as np


def get_top_k_nearest_teams_in_clusters(cluster_numbers, season_year, connection, k_nearest_teams=25):
    """
    Find the top k nearest teams to cluster centroids for given clusters.
    
    Args:
        cluster_numbers (list): List of cluster IDs to process
        season_year (int): The season year to analyze
        connection (sqlite3.Connection): Database connection
        k_nearest_teams (int): Number of nearest teams to return (default: 25)
    
    Returns:
        pd.DataFrame: DataFrame containing team names, seasons, clusters, and distances
    """
    # Load team cluster labels from CSV file
    team_cluster_df = pd.read_csv(f'Analysis/Clustering/Teams/{season_year}/KClustering/labels.csv')       

    # Query team feature data from database for the specified year range
    teams_features_df = pd.read_sql("""
        SELECT 
            team_name,
            season_year,
            adjoe AS team_adjoe,
            adjde AS team_adjde,
            (ast_pg * games_played) / (stl_pg * games_played) AS team_stltov_ratio,
            (oreb_pg * games_played * 100) / POSS AS team_oreb_per100,
            (dreb_pg * games_played * 100) / POSS AS team_dreb_per100,
            three_rate / 100 AS team_threeRate,
            ftr / 100 AS team_ftr,
            eFG / 100 AS team_eFG
        FROM Team_Seasons  
        WHERE season_year < ? AND season_year >= ?                                        
        """, con=connection, params=(season_year, season_year - 3))    
    
    # Merge cluster labels with team features
    merged_teams_df = pd.merge(team_cluster_df, teams_features_df, on=['team_name', 'season_year'])
    
    # Group teams by cluster
    teams_by_cluster = {}
    for cluster_id in cluster_numbers:
        teams_by_cluster[cluster_id] = merged_teams_df[merged_teams_df['team_cluster'] == cluster_id]

    # Initialize result DataFrame
    final_teams_df = pd.DataFrame(columns=['team_name', 'season_year', 'team_cluster', 'dist'])   

    # Process each cluster to find nearest teams to centroid
    for cluster_id, cluster_teams in teams_by_cluster.items():
        # Create DataFrame for current cluster
        cluster_distances_df = pd.DataFrame(columns=['team_name', 'season_year', 'team_cluster', 'dist'])

        # Calculate distance from each team to cluster centroid
        for team_idx, team_row in cluster_teams.iterrows():  
            # Project team features to PCA space
            projected_vector = project_to_pca(team_row[3:], season_year)
            cluster_centroid = get_centroid(season_year)                        
            
            # Calculate Euclidean distance to centroid
            distance_to_centroid = np.linalg.norm(cluster_centroid - projected_vector.values)                
            cluster_distances_df.loc[len(cluster_distances_df)] = [
                team_row['team_name'], 
                team_row['season_year'], 
                team_row['team_cluster'], 
                distance_to_centroid
            ]
        
        # Sort by distance (ascending - closest first)
        cluster_distances_df = cluster_distances_df.sort_values(by="dist", ascending=True)

        # Take all teams (could be limited to top k if needed)
        top_teams_df = cluster_distances_df.head(len(cluster_distances_df))
        
        # Add to final result if not empty
        if not top_teams_df.empty and not top_teams_df.isna().all(axis=None):
            if final_teams_df.empty:
                final_teams_df = top_teams_df.copy()
            else:
                final_teams_df = pd.concat([final_teams_df, top_teams_df], ignore_index=True)
                  
    return final_teams_df

def load_players(stat_query, connection, season_year, position):
    """
    Load player data with statistics, team info, and cluster assignments.
    
    Args:
        stat_query (str): SQL fragment specifying which statistics to select
        connection (sqlite3.Connection): Database connection
        season_year (int): The season year to analyze
        position (str): Player position (e.g., 'PG', 'SG', 'SF', 'PF', 'C')
    
    Returns:
        pd.DataFrame: DataFrame with player stats, team info, and cluster assignments
    """
    # Build comprehensive player query with team and statistical data
    player_query = f"""
    SELECT
        p.player_name,
        ps.player_id,
        ps.team_name,
        ps.season_year,
        ts.barthag_rank,
        ps.min_pg,        
        ps.bpm,
        {stat_query}
    FROM Player_Seasons ps            
    JOIN Players p
        ON ps.player_id = p.player_id
    JOIN Team_Seasons ts
        ON ts.team_name = ps.team_name AND ts.season_year = ps.season_year
    WHERE ps.season_year < ? AND ps.season_year >= ? AND ps.position = ? AND ps.bpm > -2
    """
    
    # Execute query to get player statistics
    player_stats_df = pd.read_sql(
        player_query, connection, params=(season_year, season_year - 3, position)
    )

    # Load team cluster assignments
    team_cluster_df = pd.read_csv(f'Analysis/Clustering/Teams/{season_year}/KClustering/labels.csv')

    # Load player cluster assignments
    player_cluster_df = pd.read_csv(f'Analysis/Clustering/Players/{season_year}/KClustering/player_labels_{position}.csv')
    # Remove duplicate columns to avoid merge conflicts
    player_cluster_df = player_cluster_df.drop(['team_name'], axis=1)

    # Merge player stats with team cluster information
    merged_with_team_clusters = pd.merge(
        left=player_stats_df, 
        right=team_cluster_df, 
        on=['team_name', 'season_year']
    )

    # Merge with player cluster information
    final_merged_df = pd.merge(
        left=merged_with_team_clusters, 
        right=player_cluster_df,
        on=['player_id', 'season_year', 'player_name']
    )


    final_merged_df.drop(columns=['player_id'], inplace=True)
    
    return final_merged_df

def load_players_from_cluster(stat_query, connection, season_year, cluster_id, position: str):
    """
    Load players from a specific team cluster.
    
    Args:
        stat_query (str): SQL fragment specifying which statistics to select
        connection (sqlite3.Connection): Database connection
        season_year (int): The season year to analyze
        cluster_id (int): Team cluster ID to filter by
        position (str): Player position
    
    Returns:
        pd.DataFrame: DataFrame with players from the specified cluster
    """
    # Load all players first
    all_players_df = load_players(stat_query, connection, season_year, position)
    
    # Filter by team cluster and remove unnecessary columns
    cluster_players_df = all_players_df[all_players_df['team_cluster'] == cluster_id]
    
    return cluster_players_df.drop(['team_cluster', 'team_name'], axis=1)

def load_players_from_multiple_clusters(stat_query, connection, season_year, team_cluster_ids, 
                                       player_cluster_ids, position: str, keep_metadata: bool = False, 
                                       use_top_k_teams=True):
    """
    Load players from multiple team and player clusters.
    
    Args:
        stat_query (str): SQL fragment specifying which statistics to select
        connection (sqlite3.Connection): Database connection
        season_year (int): The season year to analyze
        team_cluster_ids (list): List of team cluster IDs to include
        player_cluster_ids (list): List of player cluster IDs to include
        position (str): Player position
        keep_metadata (bool): Whether to keep team names and other metadata
        use_top_k_teams (bool): Whether to use only top k nearest teams to centroids
    
    Returns:
        pd.DataFrame: DataFrame with players from specified clusters
    
    Raises:
        ValueError: If team_cluster_ids is empty
    """
    if not team_cluster_ids:
        raise ValueError("team_cluster_ids must contain at least one cluster id.") 

    # Load all players for the position
    all_players_df = load_players(stat_query, connection, season_year, position)

    # Filter by team clusters
    team_filtered_df = all_players_df[all_players_df["team_cluster"].isin(team_cluster_ids)] 

    # Filter by player clusters (note: 'Cluster' column is player cluster, 'team_cluster' is team cluster)
    cluster_filtered_df = team_filtered_df[team_filtered_df["Cluster"].isin(player_cluster_ids)]

    # Optionally filter to only top k teams nearest to cluster centroids
    if use_top_k_teams:  
        top_teams_df = get_top_k_nearest_teams_in_clusters(team_cluster_ids, season_year, connection)                    
        final_df = pd.merge(
            cluster_filtered_df, 
            top_teams_df, 
            how="inner", 
            on=["team_name", "season_year", "team_cluster"]
        ).drop(['dist', 'season_year'], axis='columns')    
    else:
        final_df = cluster_filtered_df

    # Remove metadata columns if not requested
    if not keep_metadata:
        final_df = final_df.drop(columns=["team_name"], errors="ignore")

    return final_df.reset_index(drop=True)


def get_incoming_team_roster(connection, team_name, incoming_season_year):
    """
    Get the roster for a team in an upcoming season, including returning players and recruits.
    
    Args:
        connection (sqlite3.Connection): Database connection
        team_name (str): Name of the team
        incoming_season_year (int): The upcoming season year
    
    Returns:
        pd.DataFrame: Combined DataFrame of returning players and high school recruits
    """
    # Query for returning players from previous season
    returners_query = """
    SELECT
        p.player_id,
        p.player_name, 
        prev_ps.position,    
        prev_ps.FGA,
        prev_ps.FGM,
        prev_ps.FTA,
        prev_ps.threeM AS P3M,
        prev_ps.threeA AS P3A,                                
        prev_ps.adjoe,
        prev_ps.adrtg AS adjde,
        prev_ps.TOV,
        prev_ps.STL,                                
        prev_ps.OREB,
        prev_ps.DREB                           
    FROM Player_Seasons prev_ps
    JOIN (
        SELECT player_id
        FROM Player_Seasons                                             
        WHERE team_name = ? AND season_year = ?
    ) AS incoming_roster
      ON prev_ps.player_id = incoming_roster.player_id
    JOIN Players p
      ON prev_ps.player_id = p.player_id 
    JOIN Team_Seasons ts
        ON ts.team_name = prev_ps.team_name
       AND ts.season_year = prev_ps.season_year       
    WHERE prev_ps.season_year = ? AND prev_ps.MIN >= 40;
    """
    
    # Execute query for returning players
    returners_df = pd.read_sql(
        returners_query, 
        connection, 
        params=(team_name, incoming_season_year, incoming_season_year - 1)
    )       

    # Query for high school recruits
    recruits_query = """
    SELECT player_name, position, FGA, FGM, FTA, P3M, P3A, adjoe, adjde, TOV, OREB, DREB, bpm                 
    FROM HS_Rankings
    WHERE season_year = ? AND school_committed = ?
    """

    recruits_df = pd.read_sql(
        recruits_query, 
        connection, 
        params=(incoming_season_year - 1, team_name)
    )    

    # Combine returning players and recruits
    if not recruits_df.empty:
        return pd.concat([returners_df, recruits_df])
    else:
        return returners_df

def get_incoming_synthetic_roster(connection, team_name, incoming_season_year, player_id_to_replace):
    """
    Get the incoming roster with a specific player removed (for synthetic roster creation).
    
    Args:
        connection (sqlite3.Connection): Database connection
        team_name (str): Name of the team
        incoming_season_year (int): The upcoming season year
        player_id_to_replace (int): ID of player to remove from roster
    
    Returns:
        tuple: (roster_without_player, removed_player_info)
    """
    # Get the full incoming roster
    full_roster_df = get_incoming_team_roster(connection, team_name, incoming_season_year)    
    
    # Extract the player to be replaced
    replaced_player_df = full_roster_df[full_roster_df['player_id'] == player_id_to_replace]
    
    # Create roster without the replaced player
    remaining_roster_df = full_roster_df[full_roster_df['player_id'] != player_id_to_replace]
    
    return remaining_roster_df, replaced_player_df


def get_transfers(connection, incoming_season_year, position, player_stats_fragment, min_minutes_cutoff=80):
    """
    Get transfer players who changed teams between seasons.
    
    Args:
        connection (sqlite3.Connection): Database connection
        incoming_season_year (int): The season year to check for transfers
        position (str): Player position to filter by
        player_stats_fragment (str): SQL fragment for player statistics to select
        min_minutes_cutoff (int): Minimum minutes played threshold (default: 80)
    
    Returns:
        pd.DataFrame: DataFrame containing transfer players and their statistics
    """
    # Query to find players who transferred between seasons
    transfer_query = f""" 
    SELECT 
        p.player_name,
        ps.player_id,
        ps.season_year,
        ps.team_name AS prev_team_name,
        {player_stats_fragment}        
    FROM Player_Seasons AS ps
    JOIN Player_Seasons AS ps_next_year
        ON ps.player_id = ps_next_year.player_id
    JOIN Players p
        ON ps.player_id = p.player_id
    WHERE 
        ps.season_year = ? 
        AND ps_next_year.season_year = ? 
        AND ps.team_name != ps_next_year.team_name 
        AND ps.position = ?
        AND ps.MIN > ?
    """    

    # Execute query with parameters
    transfers_df = pd.read_sql(
        transfer_query, 
        connection, 
        params=(
            incoming_season_year - 1, 
            incoming_season_year,
            position,
            min_minutes_cutoff
        )
    )
    
    return transfers_df
