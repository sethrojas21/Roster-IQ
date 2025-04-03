import sqlite3
import pandas as pd

conn = sqlite3.connect('rosteriq.db')

c = conn.cursor()

team_query = """ 
SELECT ts.*, t.town, t.state
FROM Team_Seasons ts
INNER JOIN Teams t
ON ts.team_name = t.team_name
WHERE season_year = 2018 AND ts.team_name = "Arizona"
"""

team_roster_query = """ 
SELECT ps.*, p.*
FROM Player_Seasons2 ps
INNER JOIN Players2 p
ON ps.player_id = p.player_id
WHERE ps.team_name = "Arizona"
AND ps.season_year = 2019

"""


df = pd.read_sql(team_roster_query, conn)

print(df)

