import csv
import pandas as pd

year = '2024'

with open(f'Torvik-CSVs/Player/{year}.csv', 'r') as csvfile:
    player_df = pd.read_csv(csvfile)

with open(f'Torvik-CSVs/Team-Results/{year}.csv', 'r') as csvfile:
    teamresults_df = pd.read_csv(csvfile)

with open(f'Torvik-CSVs/Team-Final/{year}.csv', 'r') as csvfile:
    teamfinal_df = pd.read_csv(csvfile)

teams = set(player_df['team'])

headers = ['team', 'pts', 'oreb', 'dreb', 'treb', 'ast', 'stl', 'blk']
team_df = pd.DataFrame(columns=headers)

for team in list(teams):

    players = player_df[player_df['team'] == team]

    games_played = sum([int(val) for val in teamresults_df[teamresults_df['team'] == team]['record'].values[0].split('-')])

    stats= [(players[header] * players['GP']).sum() / games_played for header in headers[1:]]

    vals = [team] + stats
    
    team_df.loc[len(team_df)] = vals

merged = teamresults_df.merge(team_df, on = 'team').merge(teamfinal_df, on='team')

print(merged.columns)

    
    

    


    
