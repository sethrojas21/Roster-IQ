from bs4 import BeautifulSoup
import pandas as pd
import requests

def get_request_year(year):
    # Don't use 2021 lol
    url = None
    match year:
        case 2017 | 2018:
            url = f"https://www.jucorecruiting.com/{year}jucotop100"
        case 2019:
            url = f"https://www.jucorecruiting.com/2019-juco-basketball-top-100-rankings"
        case 2020:
            url = "https://www.jucorecruiting.com/2020-junior-college-basketball-player-rankings-top-100"
        case 2022:
            url = "https://www.jucorecruiting.com/page/show/6964746-2022-juco-top-100-"
        case 2023:
            url = "https://www.jucorecruiting.com/2023-juco-top-100"
        case 2024 | 2025:                
            url = f"https://www.jucorecruiting.com/{year}-junior-college-basketball-top-100-player-rankings"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    
    return requests.get(url, headers=headers)

def get_position(pos: str):
    if "F" in pos or "Wing" in pos:
        return "F"
    elif "G" in pos:
        return "G"
    else:
        return "C"

rankings_df = pd.DataFrame(columns=["player_name", "position", "height_inches", "ranking", "season_year", "school_committed"])

for year in range(2023, 2024):
    if year == 2021: continue
    response = get_request_year(year)
    soup = BeautifulSoup(response.text, 'html.parser')

    rows = [row.findAll('td') for row in soup.findAll('tr')][1:]
    ranking = 1
    for player in rows:
        try:
            # name = player[1].text + " " + player[2].text
            name = player[1].text
            height_arr = player[2].text.split("'")
            feet, inches = int(height_arr[0]), int(height_arr[1])
            height_int = feet * 12 + inches

            position = get_position(player[3].text)
            # position = ""
            school_commited_to = player[5].text

            rankings_df.loc[len(rankings_df)] = [name, position, height_int, ranking, year, school_commited_to]
            ranking += 1
        except Exception as e:
            print(e)     
       
    

rankings_df.to_csv("FreshmenJUCO_Rankings/jucoT100Rankings_2023.csv")