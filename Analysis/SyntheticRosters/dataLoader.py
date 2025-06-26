import pandas as pd

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
    WHERE prev_ps.season_year = ?;
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

    return pd.concat([returners_df, hs_df])

def remove_player_from_team(team_df, player_id):
    player_rmv = team_df[team_df['player_id'] == player_id]
    return team_df[team_df['player_id'] != player_id]

def get_transfers(conn, incoming_season_year, pos):
    query = """ 
    SELECT 
        p1.player_id,
        p.player_name,
        p1.season_year,               
        p1.efg_percent,
        p1.ast_percent,
        p1.oreb_percent,
        p1.dreb_percent,
        p1.tov_percent,
        p1.ft_percent,        
        p1.stl_percent,
        p1.blk_percent,
        (p1.threeA / p1.FGA) AS threeRate,
        p1.position,    
        p1.FGA,
        p1.FGM,
        p1.FTA,
        p1.threeM AS P3M,
        p1.threeA AS P3A,                                
        p1.adjoe,
        p1.adrtg AS adjde,
        p1.TOV,
        p1.STL,                                
        p1.OREB,
        p1.DREB 
    FROM Player_Seasons AS p1
    JOIN Player_Seasons AS p2
    ON p1.player_id = p2.player_id
    JOIN Players p
    ON p1.player_id = p.player_id
    WHERE p1.season_year = ? AND p2.season_year = ? AND p1.team_name != p2.team_name AND p1.position = ?"""

    return pd.read_sql(query, conn, params=(incoming_season_year - 1, 
                                                incoming_season_year,
                                                pos))
 
