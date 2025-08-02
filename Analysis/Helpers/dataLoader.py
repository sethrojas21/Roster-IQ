import pandas as pd
from Analysis.Clustering.matchTeamToCluster import project_to_pca, get_centroid
import numpy as np

def get_top_k_nearest_teams_in_clusters(cluster_nums, year, conn, k_nearest_teams = 25):
    team_cluster_df = pd.read_csv(f'Analysis/Clustering/Teams/{year}/KClustering/labels.csv')       

    teams_data_features_df = pd.read_sql("""SELECT 
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
                                        """, con=conn, params=(year, year - 3))    
    
    merged_df = pd.merge(team_cluster_df, teams_data_features_df, on=['team_name', 'season_year'])
    
    indiv_cluster_teams = {}
    
    for cluster_num in cluster_nums:
        indiv_cluster_teams[cluster_num] = merged_df[merged_df['team_cluster'] == cluster_num]

    final_df = pd.DataFrame(columns=['team_name', 'season_year', 'team_cluster', 'dist'])   

    for k, v in indiv_cluster_teams.items():
        # For each cluster find top 10 teams
        cluster_df = pd.DataFrame(columns=['team_name', 'season_year', 'team_cluster', 'dist'])

        for idx, team in v.iterrows():  
            proj_vec = project_to_pca(team[3:], year)
            centroid = get_centroid(year)                        
            dist = np.linalg.norm(centroid - proj_vec.values)                
            cluster_df.loc[len(cluster_df)] = [team['team_name'], team['season_year'], team['team_cluster'], dist]
        
        cluster_df = cluster_df.sort_values(by="dist", ascending=True)

        top_k_df = cluster_df.head(len(cluster_df))
        if not top_k_df.empty and not top_k_df.isna().all(axis=None):
            if final_df.empty:
                final_df = top_k_df.copy()
            else:
                final_df = pd.concat([final_df, top_k_df], ignore_index=True)
                  
    return final_df
    

def load_players(stat_query, conn, year, pos):
    query = f"""
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
    WHERE ps.season_year < ? AND ps.season_year >= ? AND ps.position = ?
    """
    
    rate_stats_df = pd.read_sql(
        query, conn, params= (year, year - 3, pos)
    )

    team_cluster_df = pd.read_csv(f'Analysis/Clustering/Teams/{year}/KClustering/labels.csv')

    player_cluster_df = pd.read_csv(f'Analysis/Clustering/Players/{year}/KClustering/player_labels_{pos}.csv')
    player_cluster_df = player_cluster_df.drop(['player_id', 'team_name'], axis=1)

    merge_team_cluster_df = pd.merge(left=rate_stats_df, right=team_cluster_df, on=['team_name', 'season_year'])

    merge_player_cluster_df = pd.merge(left=merge_team_cluster_df, 
                                       right=player_cluster_df,
                                       on=['player_name', 'season_year'])
    final_df = merge_player_cluster_df
    
    return final_df

def load_players_from_cluster(stat_query, conn, year, cluster_num, pos : str):
    final_df = load_players(stat_query, conn, year, pos)
    
    final_df = final_df[final_df['team_cluster'] == cluster_num]
    
    return final_df.drop(['team_cluster', 'team_name'], axis=1)

def load_players_from_multiple_clusters(stat_query, conn, year, team_cluster_nums, player_cluster_nums, pos: str, keep_meta: bool = False, top_k_teams = True):
    if not team_cluster_nums:
        raise ValueError("cluster_nums must contain at least one cluster id.") 


    final_df = load_players(stat_query, conn, year, pos)

    # Keep only rows whose cluster_num is in cluster_nums    
    final_df = final_df[final_df["team_cluster"].isin(team_cluster_nums)] 

    final_df = final_df[final_df["Cluster"].isin(player_cluster_nums)]  # cluster_num is team and Cluster is player

    if top_k_teams:  
        top_k_teams = get_top_k_nearest_teams_in_clusters(team_cluster_nums, year, conn)                    
        final_df = pd.merge(final_df, top_k_teams, how="inner", on=["team_name", "season_year", "team_cluster"]).drop(['dist', 'season_year'], axis='columns')    

    if not keep_meta:
        final_df = final_df.drop(columns=["team_name"], errors="ignore")

    return final_df.reset_index(drop=True)


def get_incoming_team_roster(conn, team_name, incoming_season_year):
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
    ) AS incomingRoster
      ON prev_ps.player_id = incomingRoster.player_id
    JOIN Players p
      ON prev_ps.player_id = p.player_id 
    JOIN Team_Seasons ts
        ON ts.team_name = prev_ps.team_name
       AND ts.season_year = prev_ps.season_year       
    WHERE prev_ps.season_year = ? AND prev_ps.MIN >= 40;
    """
    returners_df = pd.read_sql(returners_query, 
                               conn, 
                               params=(team_name, 
                                       incoming_season_year, 
                                       incoming_season_year - 1,))       

    hs_query = f"""
    SELECT player_name, position, FGA, FGM, FTA, P3M, P3A, adjoe, adjde, TOV, OREB, DREB, bpm                 
    FROM HS_Rankings
    WHERE season_year = ? AND school_committed = ?
    """

    hs_df = pd.read_sql(hs_query, conn, params=(incoming_season_year - 1, team_name))    

    return pd.concat([returners_df, hs_df]) if not hs_df.empty else returners_df

def get_incoming_synthetic_roster(conn, team_name, incoming_season_year, player_id_to_replace):
    df = get_incoming_team_roster(conn, team_name, incoming_season_year)    
    player_rmvd = df[df['player_id'] == player_id_to_replace]
    return (df[df['player_id'] != player_id_to_replace], player_rmvd)

def remove_player_from_team(team_df, player_id):
    player_rmv = team_df[team_df['player_id'] == player_id]
    return player_rmv


def get_transfers(conn, incoming_season_year, pos, ps_feature_snippet, min_cutoff = 175):
    query = f""" 
    SELECT 
        p.player_name,
        ps.player_id,
        ps.season_year,
        {ps_feature_snippet}        
    FROM Player_Seasons AS ps
    JOIN Player_Seasons AS p2
    ON ps.player_id = p2.player_id
    JOIN Players p
    ON ps.player_id = p.player_id
    WHERE 
        ps.season_year = ? 
        AND p2.season_year = ? 
        AND ps.team_name != p2.team_name 
        AND ps.position = ?
        AND ps.MIN > ?"""    

    df = pd.read_sql(query, conn, params=(incoming_season_year - 1, 
                                                incoming_season_year,
                                                pos,
                                                min_cutoff))
    return df
