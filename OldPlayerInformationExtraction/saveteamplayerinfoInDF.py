import groqtest
import extractteaminfo
import pickle
import csv
import pandas as pd
from io import StringIO
from pathlib import Path
import os
import requests
from pathlib import Path

with open('/Users/sethrojas/Documents/CodeProjects/baresearch/TeamPlayerInformation/teamOnlineInfoDict.pkl', 'rb') as file:
    teamInfoDict: dict = pickle.load(file)
    teamnames = teamInfoDict.keys()

def html_to_csvstr(html_content: str, prompt: str):
    teamInfoSnippet = extractteaminfo.get_info_from_htmlcontent(html_content)
    print("Going to make API call")
    playerinfo: str = groqtest.get_player_info_str(teamInfoSnippet, prompt)
    print(playerinfo)
    return playerinfo

### Failed or teams found already ###
# extracted_teams_path = Path(f'/Users/sethrojas/documents/codeprojects/baresearch/TeamPlayerInformation/TeamCSVs/{years}')
# extracted_teams = [file.stem for file in extracted_teams_path.glob("*csv")]
# failed_teams = ["Boise State", "Florida Gulf Coast","Campbell", "Hampton", "Chatanooga", "Evansville", "Louisiana-Lafayette", "Rider", "Rice", "Loyola Marymount", "Louisiana Tech", "Akron", "Saint Thomas (Minn.)", "Northern Arizona", "Howard", "Florida A&M",
# "Connecticut", "Kentucky", "Presbyterian"]

# failed_teams = ['Rider', 'Jacksonville', 'Citadel', "Saint Joseph's"]
# extracted_teams.extend(failed_teams)

def saveTeamData(html_content, team_name, prompt, years):
    
    # Store csv and data frame format
    try:
        playerinfo_csvstr = html_to_csvstr(html_content, prompt)
    except TypeError as typeError:
        print(typeError)

    try:
        # Try converting to data frame
        csvFileLikeObj = StringIO(playerinfo_csvstr)

        df = pd.read_csv(csvFileLikeObj, quotechar='"', quoting=csv.QUOTE_MINIMAL)

        print(df)
        
        saved_csv_filepath = f'/Users/sethrojas/documents/codeprojects/baresearch/TeamPlayerInformation/TeamCSVs/{years}/{team_name}.csv'

        try:
            df.to_csv(saved_csv_filepath, index=False)
            print("Saved " + team_name)
        except Exception as e:
            print(f"Could not convert {team_name} to csv")

    except pd.errors.ParserError as e:
        print(f"Parse Error on {team_name}: {e}")
    except Exception as e:
        print(f"Unexpected error on  {team_name}: {e}")

def mainSavedTeamHTMLs():
    teamWebsiteHTML_folderPath = Path(f'/Users/sethrojas/documents/codeprojects/baresearch/TeamWebsiteHTMLs/{years}')

    for team_html_file in list(teamWebsiteHTML_folderPath.glob('*.html')):
        team_name = team_html_file.stem

        if team_name in extracted_teams:
            print(f"skipping {team_name} because already found.")
            continue

        # Open the html file
        with open(team_html_file, 'r') as file:
            html_content = file.read()
        
        saveTeamData(html_content)

def mainGetRequestHTML():
    
    currYear = 2021
    nextYear = 2022

    teamDfFolderPath = Path(f'/Users/sethrojas/Documents/CodeProjects/baresearch/TeamPlayerInformation/TeamCSVs/{currYear}-{nextYear}')
    teamDfsFound = [file.name[:-4] for file in teamDfFolderPath.glob('*.csv')]
    
    # Failed teams
    # Added to csv - Drexel, Colorado
    # Bad - Evansville, Green Bay, Gardner-Webb, Georgia, Hampton, Iowa State, Le Moyne
    # Token - everyonen else
    # Not get right year: Kentucky
    # Failed teams
    # failed_teams_2023 = ["Arkansas", "Arkansas-Pine Bluff", "Boise State", "Bradley", "Central Connecticut", "Chicago State", "Clemson", "Colorado", 
    # "Evansville", "Gardner-Webb", "Georgia", "Green Bay", "Hampton", "Iowa State", "Le Moyne", "Louisiana-Lafayette", "Murray State", "Elon", "Drexel",
    # "New Orleans", "Nicholls State", "Presbyterian", "Saint Francis (PA)", "Wyoming"]

    # 2019-2020: Messed up Iowa State, Baylor, Kansas, Tennessee, Utah State, Western Kentucky
    # failed_teams_2020 = ["Arkansas", "Bradley", "Central Connecticut", "Clemson", "Delaware State", "Florida A&M", "Georgia", "Grambling"
    #                 "Incarnate Word", "Tennessee Tech", "Xavier"] # 2019-2020
    failed_teams = [ "Arkansas", "Bradley", "Central Connecticut", "Georgia", "Iowa State", "George Mason", "Clemson", "Notre Dame", "Baylor"]
    
    teamDfsFound.extend(failed_teams)
    
    for team_name in teamInfoDict.keys():
        if team_name in teamDfsFound:
            print("Skipping " + team_name)
            continue

        print(team_name)
        html_content = extractteaminfo.get_html_content_testyears(teamInfoDict[team_name], f'{currYear}-{nextYear}', f'{currYear}-{nextYear%2000}')
        
        saveTeamData(html_content, team_name, groqtest.getNameWeight_prompt, f'{currYear}-{nextYear}')

mainGetRequestHTML()

### TESTING ###

# with open('TeamWebsiteHTMLs/2021-2022/Arizona.html', 'r') as file:
#     html_content = file.read()
    
#     csvstr = StringIO(html_to_csvstr(html_content))
#     df = pd.read_csv(csvstr)

#     print(df)
#     print(csvstr)