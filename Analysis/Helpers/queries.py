gen_player_stats_query = """
ps.ts_percent,
ps.porpag,
ps.dporpag,
"""

g_query = """
ps.ast_percent,
ps.dreb_percent,
ps.stl_percent
"""

f_query = """
ps.ast_percent,
ps.dreb_percent,
ps.oreb_percent,
ps.blk_percent
"""

c_query = """
ps.dreb_percent,
ps.oreb_percent,
ps.blk_percent
"""

pos_stat_queries_dict = {
    "G" : g_query,
    "F" : f_query,
    "C" : c_query
}

stats_query = lambda pos_query : f"""
SELECT 
    p.player_name,
    ps.position,
    ps.season_year, 
    ps.team_name,
    {gen_player_stats_query}
    {pos_query}
FROM Player_Seasons ps
JOIN Players p ON ps.player_id = p.player_id
"""

def all_players_query(pos):
    pos_query = pos_stat_queries_dict[pos]
    return f"""
            {stats_query(pos_query)}
            WHERE ps.season_year >= ? AND ps.season_year < ?"""

def single_player_query(pos):
    pos_query = pos_stat_queries_dict[pos]
    return f"""
    {stats_query(pos_query)}
    WHERE ps.season_year = ? AND ps.player_id = ?
            """

transfer_query = """ 
SELECT 
    p.player_name,
    p1.position,
    p.player_id,
    p1.team_name,
    p2.team_name AS new_team
FROM Player_Seasons AS p1
JOIN Player_Seasons AS p2
ON p1.player_id = p2.player_id
JOIN Players p
ON p1.player_id = p.player_id
WHERE
p1.season_year = ? AND p2.season_year = ? AND p1.team_name != p2.team_name;"""

statsFromPreviousSeason = """
WITH TeamMinutesPlayed AS (
    SELECT 
        ps.team_name,
        ps.season_year,
        SUM(ps.min_pg * ps.games_played) AS total_team_minutes
    FROM Player_Seasons ps
    GROUP BY ps.team_name, ps.season_year
),

Player3PTFreq AS (
    SELECT
        ps.player_id,
        ps.season_year,
        ps.threeA * 1.0 / ps.FGA AS three_freq
    FROM Player_Seasons ps
    GROUP BY ps.player_id, ps.season_year
),

League3PTFreq AS (
    SELECT 
        ps.season_year,
        SUM(ps.threeA) * 1.0 / SUM(ps.FGA) AS league_3pt_freq
    FROM Player_Seasons ps
    GROUP BY ps.season_year
),

LeagueAdjoe AS (
    SELECT 
        ts.season_year,
        AVG(ts.adjoe) AS league_avg_adjoe
    FROM Team_Seasons ts
    GROUP BY ts.season_year
)

SELECT
    ps.player_id,
    p.player_name,
    ps.player_year,
    ps.season_year,
    ps.team_name AS prev_team_name,
    ts.barthag_rank AS prev_team_barthag_rank, -- Barthag rank of the player's previous team
    ts.cluster AS prev_team_cluster,
    ps.height_inches,
    ps.position,
    ps.bpm,
    ps.games_played,
    ps.min_pg * ps.games_played AS total_player_minutes,
    tmp.total_team_minutes,
    ps.efg_percent, 
    ps.ts_percent, 
    ps.usg_percent, 
    ps.oreb_percent, 
    ps.dreb_percent, 
    ps.ast_percent, 
    ps.tov_percent,
    ps.adrtg,
    ps.adjoe,
    ts.eFG AS team_eFG,
    (ts.adjoe - ts.adjde) AS team_adj_netrg,
    ts.adjoe AS team_adj_off,
    ts.adjde AS team_adj_def,
    ps.two_percent,
    ps.three_percent,
    ts.adjt,
    p3freq.three_freq,
    l3freq.league_3pt_freq,
    ladjoe.league_avg_adjoe
FROM Player_Seasons ps
JOIN TeamMinutesPlayed tmp
    ON ps.team_name = tmp.team_name
    AND ps.season_year = tmp.season_year
JOIN Team_Seasons ts
    ON ps.team_name = ts.team_name
    AND ps.season_year = ts.season_year
JOIN Player3PTFreq p3freq
    ON ps.player_id = p3freq.player_id
    AND ps.season_year = p3freq.season_year
JOIN Players p
    ON ps.player_id = p.player_id
JOIN League3PTFreq l3freq
    ON ps.season_year = l3freq.season_year
JOIN LeagueAdjoe ladjoe
    ON ps.season_year = ladjoe.season_year
WHERE ps.season_year = ?
AND total_player_minutes > 100;
"""