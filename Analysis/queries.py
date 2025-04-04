import sqlite3
import pandas as pd

transfer_query = """ 
SELECT 
    p.player_name,
    p1.position,
    p.player_id,
    p1.team_name
FROM Player_Seasons AS p1
JOIN Player_Seasons AS p2
ON p1.player_id = p2.player_id
JOIN Players p
ON p1.player_id = p.player_id
WHERE
p1.season_year = ?
AND p2.season_year = ?
AND p1.team_name != p2.team_name;"""

az_query = """ 
SELECT
p.player_name,
ps.min_pg
FROM
Player_Seasons AS  ps
JOIN Players AS p
ON ps.player_id = p.player_id
WHERE
ps.team_name = "Arizona"
AND ps.season_year = ?
ORDER BY
    ps.min_pg DESC
LIMIT 15;
"""

rosters_that_changed = """ 
SELECT 
    ps1.team_name, 
    COUNT(ps1.player_id) AS roster_turnover
FROM Player_Seasons AS ps1
JOIN Player_Seasons AS ps2 
    ON ps1.player_id = ps2.player_id
WHERE 
    ps1.season_year = ?
    AND ps2.season_year = ?
    AND ps1.team_name <> ps2.team_name
GROUP BY ps1.team_name
ORDER BY roster_turnover DESC;
"""

gptTransferQuery = """ 
SELECT 
    p.player_name,
    p1.player_id,
    p1.position,
    p1.team_name AS old_team,
    p2.team_name AS new_team
FROM Player_Seasons AS p1
JOIN Player_Seasons AS p2 
    ON p1.player_id = p2.player_id
JOIN Players p 
    ON p1.player_id = p.player_id
WHERE 
    p1.season_year = ?
    AND p2.season_year = ?
    AND p1.team_name <> p2.team_name;"""

gptTransferQuery2 = """
SELECT 
    p.player_name,
    p1.player_id,
    p1.position,
    p1.team_name AS old_team,
    p2.team_name AS new_team
FROM Player_Seasons AS p1
JOIN Player_Seasons AS p2 
    ON p1.player_id = p2.player_id
JOIN Players p 
    ON p1.player_id = p.player_id
WHERE 
    p1.season_year = ?
    AND p2.season_year = ?
    AND p1.team_name <> p2.team_name

UNION

SELECT 
    p.player_name,
    p1.player_id,
    p1.position,
    p1.team_name AS old_team,
    NULL AS new_team
FROM Player_Seasons AS p1
LEFT JOIN Player_Seasons AS p2 
    ON p1.player_id = p2.player_id AND p2.season_year = ?
JOIN Players p 
    ON p1.player_id = p.player_id
WHERE 
    p1.season_year = ?
    AND p2.player_id IS NULL;
"""


updatedRosterTurnover =  """
WITH Top_Players AS (
    SELECT
        ps.player_id,
        ps.team_name,
        ps.min_pg
    FROM Player_Seasons AS ps
    WHERE ps.season_year = 2023
    AND ps.min_pg > 0  -- Exclude walk-ons with 0 minutes
    ORDER BY ps.min_pg
    LIMIT 15;
)
SELECT 
    ps1.team_name, 
    COUNT(DISTINCT ps1.player_id) AS roster_turnover
FROM Player_Seasons AS ps1
JOIN Player_Seasons AS ps2 
    ON ps1.player_id = ps2.player_id
JOIN Top_Players AS tp 
    ON ps1.player_id = tp.player_id
WHERE 
    ps1.season_year = 2023
    AND ps2.season_year = 2024
    AND ps1.team_name <> ps2.team_name
GROUP BY ps1.team_name
HAVING roster_turnover > 0
ORDER BY roster_turnover DESC;
"""


all_players = """ 
    SELECT
        p.player_name,
        p.player_id,
        ps.position,
        ps.bpm,
        ps.efg_percent,
        -- ps.ts_percent,
        ps.oreb_percent,
        ps.dreb_percent,
        -- (1- ps.tov_percent),
        ps.ft_percent,
        ps.tov_percent,
        -- ps.ftr,
        -- ps.two_percent,
        -- ps.three_percent,
        -- ps.blk_percent,
        -- ps.stl_percent,
        ps.aortg,
        ps.adrtg
    FROM Player_Seasons ps
    JOIN Players p
    ON p.player_id = ps.player_id
    WHERE ps.season_year = ?
"""

all_teams = """ 
    SELECT
        ts.team_name,
        ts.eFG,
        -- ts.ts_percent,
        ts.or_percent,
        ts.dr_percent,
        -- (1- ts.tov_percent),
        ts.ft_percent,
        ts.to_percent,
        -- ts.ftr,
        -- ts.two_percent,
        -- ts.three_percent,
        -- ts.blk_percent,
        -- ts.stl_percent,
        ts.adjoe,
        ts.adjde
    FROM Team_Seasons ts
    WHERE ts.season_year = ?
"""

positionsThatChangedOnRoster = f""" 
WITH transfer AS (
    {transfer_query[:-1]}
)

SELECT 
    t.team_name,
    GROUP_CONCAT(DISTINCT t.position) AS position_lost
FROM 
    transfer t
GROUP BY
    t.team_name;
"""

boxPlusMinusQuery1 = """
WITH TeamMinutesPlayed AS (
    SELECT 
        ps.team_name,
        ps.season_year,
        SUM(ps.min_pg * ps.games_played) AS total_team_minutes
    FROM Player_Seasons ps
    GROUP BY ps.team_name, ps.season_year
)

WITH Player3PTFreq AS (
    SELECT
        ps.player_id,
        ps.season_year,
        ps.threeA/ps.FGA AS three_freq
    FROM Player_Seasons ps
)

WITH League3PTFreqAS (
    SELECT 
        AVG(three_rate) AS league_3pt_freq,
        season_year
    FROM Team_Seasons
)

SELECT
    ps.player_id,
    ps.height_inches,
    ps.weight_lbs,
    ps.position,
    ps.games_played,
    ps.min_pg * ps.games_played AS total_minutes,
    tmp.total_team_minutes,
    ps.efg_percent, 
    ps.ts_percent, 
    ps.usg_percent, 
    ps.oreb_percent, 
    ps.dreb_percent, 
    ps.ast_percent, 
    ps.tov_percent,
    ts.eFG AS team_eFG,
    (ts.adjoe - ts.adjde) AS team_adj_netrg,
    ts.adjoe AS team_adj_off,
    p3freq.three_freq
FROM Player_Seasons ps
JOIN TeamMinutesPlayed tmp
    ON ps.team_name = tmp.team_name
    AND ps.season_year = tmp.season_year
JOIN Team_Seasons ts
    ON ps.team_name = team_name
    AND ps.season_year = ts.season_year
JOIN Player3PTFreq p3freq
    ON ps.player_id = p3freq.player_id
    AND ps.season_year = p3freq.season_year
WHERE ps.season_year = ?;
"""