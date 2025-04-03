import sqlite3
import pandas as pd

def pd_standardize_columns(df, columns):
    for column in columns:
        
        name = "std_" + column  # Prefix to indicate standardized columns
        mean = df[column].mean()
        std = df[column].std()
        
        # Avoid division by zero in case of zero variance
        df[name] = (df[column] - mean) / std if std != 0 else 0


conn = sqlite3.connect('rosteriq.db')
cursor = conn.cursor()

def calcInvidualORTG(playerDF, player, Team_ORBp):
    Team_MP = (playerDF['MIN']).sum()
    Team_AST = (playerDF['AST']).sum()
    Team_PTS = playerDF['PTS'].sum().iloc[0]
    Team_FGA = (playerDF['FGA']).sum()
    Team_FGM = (playerDF['FGM']).sum()
    Team_FTM = (playerDF['FTM']).sum().iloc[0]
    Team_FTA = (playerDF['FTA']).sum()
    Team_ORB = playerDF['OREB'].sum()
    Team_3PM = playerDF['threeM'].sum()
    Team_TOV = (playerDF['AST'] / playerDF['ast_tov_r']).sum()
    PTS = player['PTS'].iloc[0]
    FGA = player['FGA']
    FGM = player['FGM']
    TOV = player['AST'] / player['ast_tov_r'] if player['ast_tov_r'] != 0 else 0
    FTM = player['FTM'].iloc[0]
    FTA = player['FTA']
    ORB = player['OREB']
    MP = player['MIN']
    AST = player['AST']
    P3M = player['threeM']

    # Scroing possession
    qAST = ((MP / (Team_MP / 5)) * (1.14 * ((Team_AST - AST) / Team_FGM))) + ((((Team_AST / Team_MP) * MP * 5 - AST) / ((Team_FGM / Team_MP) * MP * 5 - FGM)) * (1 - (MP / (Team_MP / 5))))
    try:
        FG_Part = FGM * (1 - 0.5 * ((PTS - FTM) / (2 * FGA)) * qAST)
    except:
        FG_Part = 0
    AST_Part = 0.5 * (((Team_PTS - Team_FTM) - (PTS - FTM)) / (2 * (Team_FGA - FGA))) * AST
    try:
        FT_Part = (1-(1-(FTM/FTA))**2)*0.4*FTA
    except:
        FT_Part = 0
    Team_Scoring_Poss = Team_FGM + (1 - (1 - (Team_FTM / Team_FTA))**2) * Team_FTA * 0.4
    Team_Playp = Team_Scoring_Poss / (Team_FGA + Team_FTA * 0.4 + Team_TOV)
    Team_ORB_Weight = ((1 - Team_ORBp) * Team_Playp) / ((1 - Team_ORBp) * Team_Playp + Team_ORBp * (1 - Team_Playp))
    ORB_Part = ORB * Team_ORB_Weight * Team_Playp
    ScPoss = (FG_Part + AST_Part + FT_Part) * (1 - (Team_ORB / Team_Scoring_Poss) * Team_ORB_Weight * Team_Playp) + ORB_Part

    # Missed FG and FT Possessions
    FGxPoss = (FGA - FGM) * (1 - 1.07 * Team_ORBp)
    try:
        FTxPoss = ((1 - (FTM / FTA))**2) * 0.4 * FTA
    except:
        FTxPoss = 0

    TotPoss = ScPoss + FGxPoss + FTxPoss + TOV

    # Individual Points Produced
    try:
        PProd_FG_Part = 2 * (FGM + 0.5 * P3M) * (1 - 0.5 * ((PTS - FTM) / (2 * FGA)) * qAST)
    except:
        PProd_FG_Part = 0
    PProd_AST_Part = 2 * ((Team_FGM - FGM + 0.5 * (Team_3PM - P3M)) / (Team_FGM - FGM)) * 0.5 * (((Team_PTS - Team_FTM) - (PTS - FTM)) / (2 * (Team_FGA - FGA))) * AST
    PProd_ORB_Part = ORB * Team_ORB_Weight * Team_Playp * (Team_PTS / (Team_FGM + (1 - (1 - (Team_FTM / Team_FTA))**2) * 0.4 * Team_FTA))

    PProd = (PProd_FG_Part + PProd_AST_Part + FTM) * (1 - (Team_ORB / Team_Scoring_Poss) * Team_ORB_Weight * Team_Playp) + PProd_ORB_Part
    
    ortg = 100 * (PProd / TotPoss) if TotPoss != 0 else 0
    return ortg


for year in range(2018, 2025):
    teams = pd.read_sql("""SELECT team_name, or_percent FROM Team_Seasons WHERE season_year = ?""", conn, params=(year,))
    teamNames = set(teams['team_name'])
    df = pd.DataFrame(columns=['player_name', 'player_id','team_name', 'ortg'])

    for index, team in teams.iterrows():
        team_name = team['team_name']
        playerAzQuery = f""" 
        SELECT 
        p.player_name,
        p.player_id,
        ps.games_played,
        ps.FGA,
        ps.FGM,
        ps.threeM,
        ps.threeA,
        ps.pts_pg,
        ps.FTM,
        ps.PTS,
        ps.min_pg,
        ps.ast_tov_r,
        ps.MIN,
        ps.PTS,
        ps.OREB,
        ps.FTA,
        ps.FTM,
        ps.AST,
        ps.oreb_pg
        FROM Player_Seasons2 ps
        JOIN Players2 p ON p.player_id = ps.player_id
        WHERE ps.season_year = ?
        AND ps.team_name = ?;
        -- ORDER BY ps.MIN DESC
        -- LIMIT 12;
        """

        Team_ORBp = team['or_percent']

        playerDF = pd.read_sql(playerAzQuery, conn, params=(year, team_name))
        print(team_name)
        for index, player in playerDF.iterrows():
            ortg = calcInvidualORTG(playerDF, player, Team_ORBp / 100)
            df.loc[len(df)] = [player['player_name'], player['player_id'], team_name, ortg]
        
        
    #Add to database
    for _, row in df.iterrows():
        print("Adding", row['player_name'])
        cursor.execute(
            """ 
            UPDATE Player_Seasons2
            SET ortg = ?
            WHERE player_id = ? AND season_year = ?;
            """
        ,(row['ortg'], row['player_id'], year))

    conn.commit()