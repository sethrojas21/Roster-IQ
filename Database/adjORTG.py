import sqlite3
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

conn = sqlite3.connect('rosteriq.db')

cursor = conn.cursor()

for year in range(2018, 2025):
    query = """ 
    SELECT 
    team_name,
    sos
    FROM Team_Seasons
    WHERE season_year = ?
    """

    sosDf = pd.read_sql(query, conn, params=(year,))
    avgsos = sosDf['sos'].mean()
    stdsos = sosDf['sos'].std()


    playerDF = pd.read_sql(
        """ 
        SELECT
        player_id,
        team_name,
        ortg
        FROM Player_Seasons2
        WHERE season_year = ?
        """,
        conn,
        params = (year,)
    )

    # Merge player ORtg with team SOS
    mergedDF = playerDF.merge(sosDf, on='team_name', how='left')

    # Drop NaN values (in case some teams are missing SOS)
    mergedDF = mergedDF.dropna(subset=['ortg', 'sos'])

    # Fit linear regression ORtg ~ SOS
    X = mergedDF[['sos']].values  # Independent variable (SOS)
    y = mergedDF['ortg'].values  # Dependent variable (ORTG)

    reg = LinearRegression().fit(X, y)
    lambda_ = reg.coef_[0]  # Slope of SOS in predicting ORTG

    # Adjust ORtg using this lambda
    playerDF['aortg'] = playerDF['ortg'] + lambda_ * ((playerDF['team_name'].map(sosDf.set_index('team_name')['sos']) - avgsos))

    pQ = """ 
    UPDATE Player_Seasons2
    SET aortg = ?
    WHERE player_id = ?
    AND season_year = ?
    """

    for _, player in playerDF.iterrows():
        cursor.execute(pQ, (player['aortg'], player['player_id'], year))

conn.commit()

