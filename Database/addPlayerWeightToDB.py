import sqlite3
from pathlib import Path
from rapidfuzz import process, fuzz
import pandas as pd
import math

"""
NaNs: SF, BYU, WIU, South Dakota, East Tenn St., Missour-KC, Maine, Tulane, Miami(Fla.), SJSU, Evansville,  UCR, TEnn St., Mt St Marys, Missouri St, Merrimack, Green Bay,
"""

# Open folders
def getFolderPath(folder, fileType = "pkl"):
    
    folderPath = Path(folder)

    return list(folderPath.glob(f'*.{fileType}'))

def get_best_match(name, compareSet, score_cutoff=70):
    result = process.extractOne(name, compareSet, scorer=fuzz.WRatio, score_cutoff=score_cutoff)
    if result:
        match, score, _ = result
        return match
    return None

def addTeamWeightToDB(savedTeamDF, dbTeamDF, teamName, year, cursor, conn):

    playerNamesSet = set(dbTeamDF[dbTeamDF['team_name'] == teamName]['player_name'].values) 
    
    countPlayersSkipped = 0
    for _, player in savedTeamDF.iterrows():
        
        # Get the player info
        try:
            player_name = player['name']
            weight = player['wt']        
        except:
            player_name = player.iloc[0]
            weight = player.iloc[1]
            print("Switching here", weight, player_name)

        if not player_name: 
            print("No player name")
            continue # If none then skip 

        # Check weight and name for validity
        try:        
            if type(weight) == str:
                weight = int(weight.strip().strip('"').strip('"'))
        except:
            print("Failed to parse weight for " + player_name)
            continue
        
        # Check if a number
        if not math.isnan(weight): 
            weight = int(weight)
        else:
            countPlayersSkipped += 1
            print("Skipping because NaN for player", player_name)
            continue
     
        # Get the player in the list if found
        if player_name in list(dbTeamDF['player_name']):
            best_match_name = player_name
        else:
            # If not then search for a similar name                       
            best_match_name = get_best_match(player_name, playerNamesSet)

            if not best_match_name: 
                countPlayersSkipped += 1
                print(f"Skipping because no name found on team for {player_name}")
                continue # Skip if found none

            # print(f"Found {best_match_name} for OG name of {player_name}")

        # Check if weight is already set
        current_weight = dbTeamDF[dbTeamDF['player_name'] == best_match_name]['weight_lbs'].iloc[0]
        if current_weight and current_weight > 0:
            # print("Skipping " + best_match_name + " because weight already found")
            continue  # Skip if weight is already set

        # Update only if weight is missing
        addWeightQuery = """
            UPDATE Player_Seasons
            SET weight_lbs = ?
            WHERE season_year = ?
            AND player_id = (SELECT player_id FROM Players WHERE player_name = ?);
        """
        print("Adding " + best_match_name)
        cursor.execute(addWeightQuery, (weight, year, best_match_name))
    # if countPlayersSkipped > len(savedTeamDF) // 2:
    #     print("FAILEDDDDDDDD", teamName, "**********F*F*FSFHDFHSDFSDUFDSFS*FD*")

def getPlayerInfoDB(conn, nextYear):
    playerWeightsQuery = \
        """
        SELECT p.player_id, p.player_name, ps.weight_lbs, ps.team_name
        FROM Player_Seasons ps
        JOIN Players p ON ps.player_id = p.player_id
        WHERE ps.season_year = ?
        """
    return pd.read_sql(playerWeightsQuery, conn, params=(nextYear,))

def main():
    
    conn = sqlite3.connect('rosteriq.db')
    cursor = conn.cursor()

    for currYear in range(2021, 2022):
        nextYear = currYear + 1

        # Pandas data frame of player database
        playerInfoDB = getPlayerInfoDB(conn, nextYear)

        fileType = "csv"
        folderPath = f'/Users/sethrojas/Documents/CodeProjects/baresearch/TeamPlayerInformation/TeamCSVs/{currYear}-{nextYear}'
        savedTeamFileList = getFolderPath(folderPath, fileType)
        

        for teamInfoFile in savedTeamFileList:
            teamName = str(teamInfoFile).split("/")[-1][:-4]

            try:
                with open(teamInfoFile, 'r') as file:
                    savedTeamDf = pd.read_csv(file)
            except:
                print(teamName)
                break
            
            dbTeamDF = playerInfoDB[playerInfoDB['team_name'] == teamName]

            print("Processing " + teamName + "******")
            
            addTeamWeightToDB(savedTeamDf, dbTeamDF, teamName, nextYear, cursor, conn)

    print("Commited players")        
    conn.commit()

### RUN ###
# main()


