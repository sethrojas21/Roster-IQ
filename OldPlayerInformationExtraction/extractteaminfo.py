import requests
from bs4 import BeautifulSoup
import pickle
import tiktoken
import failedTeamLinks

MIN_LENGTH = 1250

def get_info_from_htmlcontent(content, keywords = ["lbs", "Guard", "Freshmen", "Sophmore", "Junior", "Senior", "Forward", "Full Bio"], wantCoach = False):
    soup = BeautifulSoup(content, 'html.parser')
    unique_players = set()
    
    potential_sections = soup.find_all(
        lambda tag: tag.name in ["table", 'div', 'li', 'span'] and any(keyword in tag.get_text() for keyword in keywords)
    )
    
    for section in potential_sections:
        text = section.get_text(strip=True)
        if text not in unique_players:  # Only process if it's not already seen
            unique_players.add(text)
    
    filtered_snippets = [snippet for snippet in unique_players if MIN_LENGTH < len(snippet)]
    
    if unique_players:
        shortest_snippet = min(filtered_snippets, key=len)
        print(shortest_snippet)
        ind = shortest_snippet.lower().find("coach")

        if ind != -1:
            # Reverse this if we want to search for coach or not
            if wantCoach:
                shortest_snippet = shortest_snippet[ind:]
            else:
                shortest_snippet = shortest_snippet[:ind]
            
        
        shortest_snippet = shortest_snippet.replace("Full", " ").replace("Bio", " ")
        return shortest_snippet
    
    return None

def num_tokens_from_string(string: str, encoding_name: str = "cl100k_base") -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }  
def get_html_content_testyears(link, year1test, year2test):
    for i in range(2):
        link1, link2 = link, link

        if i == 1:
            if link[-1] == '/':
                link1 += 'season/'
                link2 += 'season/'
            else:
                link1 += '/season'
                link2 += '/season'

        if link[-1] == '/':
            link1 += year1test + '/'
            link2 += year2test + '/'
        else:
            link1 += '/' + year1test
            link2 += '/' + year2test      

        # Response 1
        response1 = getResponse(link1, year1test, headers)
        if response1 is not None:
            return response1
        
        response2 = getResponse(link2, year2test, headers)
        if response2 is not None:
            return response2


def getResponse(link, year, headers):

    response = requests.get(link, headers = headers, allow_redirects=False)
    print("Trying " + link)
    if response.status_code == 200 and not response.history:
        responseText = response.text
        if checkIfYearInHTMLText(responseText, year):
            print("here")
            return responseText       
        
def checkIfYearInHTMLText(htmlText: str, year):
    return htmlText.find(year) != -1

def get_player_info_test_years(teamName, link, year1test: str, year2test, keywords = ["lbs", "Guard", "Freshmen", "Sophmore", "Junior", "Senior", "Forward", "Center"], wantCoach = False):
    if teamName in failedTeamLinks.correctedLinks:
        print("here")
        content = requests.get(failedTeamLinks.correctLinkForTeams(teamName, int(year1test[:year1test.find("-")])), headers = headers, allow_redirects=True).content
    else:
        content = get_html_content_testyears(link, year1test, year2test)
    if wantCoach:
        keywords = ['Coach', "Assistant", "Head"]
    return get_info_from_htmlcontent(content, keywords=keywords, wantCoach=wantCoach)        

def get_player_info_snippet(link, keywords = ["lbs", "Guard", "Freshmen", "Sophmore", "Junior", "Senior", "Forward", "Center"]):
    response = requests.get(link)
    
    if response.status_code == 200:
        return get_info_from_htmlcontent(response.text, keywords)

### TESTING ###

# with open('TeamPlayerInformation/teamOnlineInfoDict.pkl', 'rb') as file:
#     teamDict = pickle.load(file)

# failed_teams = ["Arkansas", "Arkansas-Pine Bluff", "Boise State", "Bradley", "Central Connecticut", "Chicago State", "Clemson", "Colorado", 
#     "Evansville", "Gardner-Webb", "Georgia", "Green Bay", "Hampton", "Iowa State", "Le Moyne", "Louisiana-Lafayette", "Murray State", "Elon", "Drexel",
#     "New Orleans", "Nicholls State", "Presbyterian", "Saint Francis (PA)", "Wyoming"]


# from time import sleep

# for teamName in ["Clemson"]:
#     print(teamName)
#     print(get_player_info_test_years(teamName, teamDict[teamName], "2019-2020", "2019-20"))
#     # try:
        
#     # except e:
#     #     print(e)
#     sleep(1)