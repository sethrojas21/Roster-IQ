import sqlite3
import pandas as pd

conn = sqlite3.connect('rosteriq.db')

team_query = """
            SELECT                
                team_name,                
                season_year
            FROM Team_Seasons;
            """

all_teams_df = pd.read_sql(team_query, conn)

all_teams_df['POSS'] = None

cursor = conn.cursor()

i = 0
for index, team_df in all_teams_df.iterrows():
    TEAM_NAME = team_df['team_name']
    SEASON_YEAR = team_df['season_year']
    players_on_team_query = """SELECT FGA, FTA, TOV, OREB 
                            FROM Player_Seasons 
                            WHERE team_name = ? AND season_year = ?;"""
    players_on_team_df = pd.read_sql(players_on_team_query, conn, params=(TEAM_NAME, SEASON_YEAR))
    FGA = players_on_team_df['FGA']
    FTA = players_on_team_df['FTA']
    TOV = players_on_team_df['TOV']
    OREB = players_on_team_df['OREB']
    total_team_poss = int(FGA.sum() + 0.44 * FTA.sum() + TOV.sum() - OREB.sum())
    all_teams_df.at[index, 'POSS'] = total_team_poss
    cursor.execute("""
                UPDATE Team_Seasons
                SET POSS = ?
                WHERE team_name = ? AND season_year = ?;
                    """, (total_team_poss, TEAM_NAME, SEASON_YEAR))
    
    if i == 50:
        conn.commit()
        print("Committed a 50 Batch")
        i = 0
    else:
        i += 1
        
conn.commit()
print("Final Commit")
print(all_teams_df)