import pandas as pd
import sqlite3

conn = sqlite3.connect('rosteriq.db')
info_df = pd.read_csv('Analysis/Helpers/CSV/availTransferTeams.csv')
columns = ['season_year','player_name', 'player_id', 'player_year', 'team_name', 'position', 'height_inches']
final_df = pd.DataFrame(columns=columns)

for _, row in info_df.iterrows():
    team_name = row['team_name']
    season_year = row['season_year']
    player_id = row['player_id']
    query = """
    SELECT 
        p.player_name,
        ps.player_id,
        ps.player_year,
        ps.position,
        ps.height_inches
    FROM Player_Seasons ps
    JOIN Players p
        ON ps.player_id = p.player_id
    WHERE ps.player_id = ? AND ps.season_year = ?
    """

    player_data = pd.read_sql(sql = query,
                              con = conn,
                              params=(player_id, season_year)).iloc[0]
    
    player_year = player_data['player_year']
    position = player_data['position']
    height_inches = player_data['height_inches']
    player_name = player_data['player_name']
    player_id = player_data['player_id']

    new_row = [season_year, player_name, player_id, player_year, team_name, position, height_inches]
    final_df.loc[len(final_df)] = new_row

final_df = final_df.sort_values(by=["season_year", "player_name"])

final_df.to_csv('Streamlit/Data/CSV/home_df.csv', index=False)