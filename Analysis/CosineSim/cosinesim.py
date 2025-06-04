import numpy as np
import heapq
from sklearn.preprocessing import StandardScaler
import sqlite3
import pandas as pd
import Analysis.PredictBPM.queries as queries

conn = sqlite3.connect('rosteriq.db')

c = conn.cursor()

def pd_normalize_lst(df, column):
    max_val = df[column].abs().max()
    return list(df[column] / max_val) if max_val != 0 else list(np.zeros(len(df)))

def pd_normalize_columns(df, columns):
    for column in columns:
        
        name = "norm_" + column
        df[name] = pd_normalize_lst(df, column)
    
def pd_standardize_columns(df, columns):
    for column in columns:
        
        name = "std_" + column  # Prefix to indicate standardized columns
        mean = df[column].mean()
        std = df[column].std()
        
        # Avoid division by zero in case of zero variance
        df[name] = (df[column] - mean) / std if std != 0 else 0


def isPlayerInTop(transferPlayersDF, team, cosine=False, posNeed=False, oneStdAway=False, posBpm=False, limit=1, percentage = 0.25):
    team_name = team.iloc[0]
    team_data = np.array(team[cutoff:])  # Convert team row to NumPy array

    # Filter players based on position if needed
    if posNeed and team_name in teamPositionsNeeded:
        posNeeded = list(teamPositionsNeeded[team_name])        
        transferPlayersDF = transferPlayersDF.loc[transferPlayersDF['position'].isin(posNeeded)]

    playersCSscore = {}

    for row in transferPlayersDF.itertuples():
        player_data = np.array(row[cutoff:])  # Convert row tuple to NumPy array

        # Filter out players really bad in at least one stat if required
        if oneStdAway and np.any(player_data < -1):
            continue

        # Compute cosine similarity
        cosine_similarity = computeCosineSimilarity(player_data, team_data)

        # Store score if it meets criteria
        if (cosine and cosine_similarity > 0) or (not cosine and cosine_similarity < 0):
            if posBpm and row[4] <= 0:
                continue
            playersCSscore[cosine_similarity] = row[1]

    # Get top `limit` scores
    if not playersCSscore:
        return 0  # No valid players

    if percentage is not None:
        limit = int(len(transferPlayersDF) * percentage)

    top_scores = heapq.nlargest(limit, playersCSscore.keys()) if cosine else heapq.nsmallest(limit, playersCSscore.keys())
    playersChosen = set(transfersDF[transfersDF['new_team'] == team_name]['player_name'])

    # Check if any top player is in `playersChosen`
    for csScore in top_scores:
        if playersCSscore[csScore] in playersChosen:
            teamsInLimit.append(team_name)
            
            return 1
                
    return 0

def computeCosineSimilarity(player_data, team_data):
    numerator = np.dot(player_data, team_data)
    denominator = (np.linalg.norm(player_data) * np.linalg.norm(team_data))
    cosine_similarity = numerator / denominator if denominator != 0 else 0
    
    return cosine_similarity


for i in range(2):
    header = ""
    val = i % 5
    cosine = True if i >= 1 else False
    posNeed = False
    oneStdAway = False
    posBpm = False
    # posNeed = True if val == 1 or val == 4 else False
    # oneStdAway = True if val == 2 or val == 4 else False
    # posBpm = True if val == 3 or val == 4 else False 

    sumTeamsInLimit = {}
    teamsInLimit = []
    for year in range(2018, 2024):
        curr_year = year
        next_year = year + 1

        allPlayersDF = pd.read_sql(queries.all_players, conn, params=(curr_year,))
        allTeamsDF = pd.read_sql(queries.all_teams, conn, params=(curr_year,))

        transfersDF = pd.read_sql(queries.gptTransferQuery, conn, params=(curr_year,next_year))
        transferIDs = list(transfersDF['player_id'])
        teamsWithRosterChanges = set(list(transfersDF['old_team']) + list(transfersDF['new_team']))

        pd_standardize_columns(allPlayersDF, allPlayersDF.columns[4:])
        pd_standardize_columns(allTeamsDF, allTeamsDF.columns[1:])

        normTransferPlayers = allPlayersDF[allPlayersDF['player_id'].isin(transferIDs)]
        normChangedTeams = allTeamsDF[allTeamsDF['team_name'].isin(teamsWithRosterChanges)]

        # Reverse neccesary statistics
        normTransferPlayers.loc[:, "std_adrtg"] = -normTransferPlayers["std_adrtg"]
        normChangedTeams.loc[:, "std_adjde"] = -normChangedTeams["std_adjde"]
        normTransferPlayers.loc[:, "std_dreb_percent"] = -normTransferPlayers["std_dreb_percent"]
        normChangedTeams.loc[:, "std_dr_percent"] = -normChangedTeams["std_dr_percent"]
        normTransferPlayers.loc[:, "std_tov_percent"] = -normTransferPlayers["std_tov_percent"]
        normChangedTeams.loc[:, "std_to_percent"] = -normChangedTeams["std_to_percent"]

        cutoff = -7
        teamPositionsNeeded = {}

        for _, row in transfersDF.iterrows():
            old_team = row['old_team']
            position = row['position']

            if old_team in teamPositionsNeeded:
                teamPositionsNeeded[old_team].add(position)
            else:
                teamPositionsNeeded[old_team] = set()
        

        # Compute sum in a vectorized manner
        
        total = sum(isPlayerInTop(normTransferPlayers, team, cosine, posNeed, oneStdAway, posBpm, limit=1, percentage = 0.25) for _, team in normChangedTeams.iterrows())
        sumTeamsInLimit[total] = year
        # print(f"{curr_year}-{next_year}")
        # print(total, teamsInLimit)
    
    
    print(f"Cosine: {cosine}, PosNeeded: {posNeed}, oneStdAway: {oneStdAway}, posBpm: {posBpm}")
    print(f"Mean: {sum(sumTeamsInLimit.keys()) / len(sumTeamsInLimit)}")
    min_ = min(sumTeamsInLimit.keys())
    print(f"Min: {min_} in {sumTeamsInLimit[min_]}")
    max_ = max(sumTeamsInLimit.keys())
    print(f"Max: {max_} in {sumTeamsInLimit[max_]}")
    print(teamsInLimit)


        