import pandas as pd

def load_players_from_cluster(stat_query,conn, year, cluster_num, pos : str):
    query = f"""
    SELECT
        p.player_name,
        ps.player_id,
        ps.team_name,
        ps.season_year,
        {stat_query}
    FROM Player_Seasons ps            
    JOIN Players p
        ON ps.player_id = p.player_id
    WHERE ps.season_year < ? AND ps.position = ?
    """
    
    rate_stats_df = pd.read_sql(
        query, conn, params= (year, pos)
    )

    team_cluster_df = pd.read_csv(f'Analysis/Clustering/ClusterData/{year}/teamSeasonClusterLabel.csv')

    final_df = pd.merge(left=rate_stats_df, right=team_cluster_df, on=['team_name', 'season_year'])
    
    return final_df.drop(['cluster_num', 'team_name'], axis=1)

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


def get_transfers(conn, incoming_season_year, pos, ps_feature_snippet, min_cutoff = 250):
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
