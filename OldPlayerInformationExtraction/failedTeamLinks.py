correctedLinks = {
    "Arkansas": "https://arkansasrazorbacks.com/sport/m-baskbl/roster/?season={curr_year}-{next_short_year}",
    "Clemson" : "https://clemsontigers.com/sports/mens-basketball/roster/season/{curr_year}/",
    "Central Connecticut" : "https://ccsubluedevils.com/sports/mbkb/{curr_year}-{next_short_year}/roster?jsRendering=true",
    "LSU" : "https://lsusports.net/sports/mb/roster/season/{curr_year}-{next_short_year}/"
}

def correctLinkForTeams(teamName, curr_year):
    next_year = curr_year + 1  
    curr_short_year = curr_year % 2000  
    next_short_year = next_year % 2000  
    
    template = correctedLinks.get(teamName)

    if template:
        return template.format(curr_year=curr_year, curr_short_year=curr_short_year, next_year=next_year, next_short_year=next_short_year)
    return None  # Return None if the team isn't found