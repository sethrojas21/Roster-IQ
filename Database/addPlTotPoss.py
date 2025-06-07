import sqlite3
import pandas as pd

conn = sqlite3.connect('rosteriq.db')
cursor = conn.cursor()

all_team_query = "SELECT team_name, season_year, POSS, games_played AS GP FROM Team_Seasons;"
all_team_df = pd.read_sql(all_team_query, conn)

i = 0
for index, team_df in all_team_df.iterrows():
    team_possessions = team_df['POSS']
    team_minutes = team_df['GP'] * 40

    TEAM_NAME = team_df['team_name']
    SEASON_YEAR = team_df['season_year']

    players_on_team_df = pd.read_sql("""
    SELECT
        p.player_name,
        ps.player_id,
        ps.MIN
    FROM Player_Seasons ps
    JOIN Players p ON ps.player_id = p.player_id
    WHERE team_name = ? AND season_year = ?;
    """, conn, params=(TEAM_NAME, SEASON_YEAR))    

    for index, player in players_on_team_df.iterrows():
        PLAYER_POSS = round(team_possessions * (player['MIN'] / team_minutes))
        PLAYER_ID = player['player_id']        
        cursor.execute("""
                       UPDATE Player_Seasons
                       SET POSS = ?
                       WHERE player_id = ? AND season_year = ?""",
                       (PLAYER_POSS, PLAYER_ID, SEASON_YEAR))
    
    if i == 10:
        conn.commit()
        print(f"Committed Batch of 10. Currently at index {index}")
        i = 0
    else:
        i += 1

conn.commit()

