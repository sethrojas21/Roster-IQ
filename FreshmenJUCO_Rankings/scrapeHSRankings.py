from bs4 import BeautifulSoup
import pandas as pd
import requests

def get_request_year(year):
    url = f"https://www.espn.com/college-sports/basketball/recruiting/playerrankings/_/class/{year}/order/true"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    return requests.get(url, headers=headers)

def get_position(pos: str):
    if "F" in pos:
        return "F"
    elif "G" in pos:
        return "G"
    else:
        return "C"

rankings_df = pd.DataFrame(columns=["player_name", "position", "height_inches", "ranking", "season_year", "school_committed"])

for year in range(2017, 2026):
    print("Starting ", year)
    response = get_request_year(year)
    soup = BeautifulSoup(response.text, 'html.parser')

    rows = [row.findAll('td') for row in soup.findAll('tr')][1:]

    ranking_num = 1

    for player in rows:
        try:
            name = player[1].find("strong").text
            position_raw = player[2].find('b').text
            position = get_position(position_raw)
            height_arr = player[4].text.strip().split("'")
            feet, inches = int(height_arr[0]), int(height_arr[1])
            height_int = feet * 12 + inches
            school_str = player[8].find("span").text

            rankings_df.loc[len(rankings_df)] = [name, position, height_int, ranking_num, year, school_str]
            ranking_num += 1
        except Exception as e:
            print(f"Skipping row due to error: {e}")
        
    print("Finishing ", year)
    

print(rankings_df)        
rankings_df.to_csv("espnT100Recruits_2017-2025.csv")