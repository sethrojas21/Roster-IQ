import groqtest
import extractteaminfo
import pickle
import csv
import pandas as pd
from io import StringIO

with open('pdold.csv', 'r') as file:
    playerdirectory = pd.read_csv(file)

with open('TeamPlayerInformation/teamnames.pkl', 'rb') as file:
    teamnames = pickle.load(file)

with open('TeamPlayerInformation/teamlinks2021-22.pkl', 'rb') as file:
    teamlinks = pickle.load(file)


for team in teamnames[:45]:
    # Get the team link
    teamLink = teamlinks[team]

    # Get the team info snippet
    teamInfoSnippet = extractteaminfo.get_player_info_snippet(teamLink)
    # Extract player info and make a string that is in form of a csv
    playerinfo: str = groqtest.get_player_info_str(teamInfoSnippet)

    print(playerinfo)
    pidf = pd.read_csv(StringIO(playerinfo))


    print(pidf)
    # ['Player', 'Position', 'Height', 'Weight', 'Year', 'Homestate']
    for player in pidf['Player'].values:
        if player in playerdirectory['Player'].values:

            ind = playerdirectory[playerdirectory['Player'] == player]

            len_ht = len(ind['Ht'].values[0])
            len_class = len(str(ind['Class'].values[0]))
            len_pos = ind['Pos'].values[0]
            len_wt = len(ind['Wt'].values[0])
            len_hs = len(ind['Hometown'].values[0])


            if len_ht < 2:
                playerdirectory.loc[ind.index, 'Ht'] = pidf[pidf['Player'] == player]['Height'].values[0]
            if len_class < 2:
                playerdirectory.loc[ind.index, 'Class'] = pidf[pidf['Player'] == player]['Year'].values[0]
            if len_pos == "0":
                playerdirectory.loc[ind.index, 'Pos'] = pidf[pidf['Player'] == player]['Position'].values[0]
            if len_wt < 2:
                playerdirectory.loc[ind.index, 'Wt'] = pidf[pidf['Player'] == player]['Weight'].values[0]
            if len_hs < 2:
                playerdirectory.loc[ind.index, 'Hometown'] = pidf[pidf['Player'] == player]['Homestate'].values[0]

            
with open('pdnew.csv', 'w') as file:
    playerdirectory.to_csv(file, index=False)

    

    

