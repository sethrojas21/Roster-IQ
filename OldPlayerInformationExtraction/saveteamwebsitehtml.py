import requests
from extractteaminfo import get_html_content_testyears
import pickle


with open('/Users/sethrojas/Documents/CodeProjects/baresearch/TeamPlayerInformation/teamOnlineInfoDict.pkl', 'rb') as file:
    teamInfoDict = pickle.load(file) 


for team_name in teamInfoDict:
    html_content = get_html_content_testyears(teamInfoDict[team_name], '2020-2021', '2020-21')

    with open(f'TeamWebsiteHTMLs/2020-2021/{team_name}.html', 'w') as file:
        if html_content is not None:
            print(f'Saved {team_name}.html')
            file.write(html_content)
        else:
            file.write('')