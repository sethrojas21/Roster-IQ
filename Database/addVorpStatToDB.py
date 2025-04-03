import sqlite3
import pandas as pd

conn = sqlite3.connect('rosteriq.db')

teamQuery = \
"""

WITH TeamMinutesPlayed AS (
    SELECT
        ps.team_name,
        ps.season_year,
        SUM(ps.min_pg * ps.games_played) AS total_team_minutes
    FROM Player_Seasons ps
    GROUP BY ps.team_name, ps.season_year
)
SELECT
    ps.player_id,
    p.player_name,
    ps.team_name,
    ps.bpm,
    tmp.total_team_minutes,
    ps.min_pg,
    ps.games_played,
    ts.record
FROM Player_Seasons ps
JOIN Players p
    ON ps.player_id = p.player_id
JOIN TeamMinutesPlayed tmp
    ON ps.team_name = tmp.team_name
    AND ps.season_year = tmp.season_year
JOIN Team_Seasons ts
    ON ps.team_name = ts.team_name
    AND ps.season_year = ts.season_year
WHERE ps.season_year = ?;
"""

cursor = conn.cursor()

def add_columns():
    cursor.execute("""ALTER TABLE Player_Seasons ADD COLUMN vorp FLOAT;""")
    cursor.execute("""ALTER TABLE Player_Seasons ADD COLUMN vorp_rank INT;""")
    cursor.execute("""ALTER TABLE Player_Seasons ADD COLUMN bpm_rank INT;""")
    conn.commit()

def add_data():
    for year in range(2022, 2025):

        df = pd.read_sql(teamQuery, conn, params=(year,))
        distinct_teams = df['team_name'].unique()

        for team in distinct_teams:
            team_df = df.loc[df['team_name'] == team].copy()

            team_df['total_games'] = team_df['record'].apply(lambda x: int(x.split('-')[0]) + int(x.split('-')[1]))

            team_df['vorp'] = \
                            (team_df['bpm'] - (-2)) * \
                            (team_df['min_pg'] * team_df['games_played'] / team_df['total_team_minutes']) * \
                            (team_df['games_played'] / team_df['total_games'])
            
            team_df['team_vorp_rank'] = team_df['vorp'].rank(method='max', ascending=False)
            team_df['team_bpm_rank'] = team_df['bpm'].rank(method='max', ascending=False)

            print(f"Team: {team}")
            print(team_df)

# TODO - Add vorp, vorp_rank, bpm_rank to Player_Seasons table
# TODO - add team rank as a feature to the model 
# TODO - add overall vorp and bpm rank to Player_Seasons table
