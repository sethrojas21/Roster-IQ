import sqlite3
import pandas as pd
from Analysis.Helpers.dataLoader import get_incoming_team_roster
from Analysis.Helpers.queries import transfer_query
from Analysis.config import Config

conn = sqlite3.connect('rosteriq.db')
team_names = pd.read_sql("SELECT team_name FROM Teams", conn)['team_name'].tolist()

avail_teams_df = pd.DataFrame(columns=['team_name', 'season_year', 'player_id'])

transfer_dfs_dict = {}
start_year, end_year = Config.START_YEAR, Config.END_YEAR_EXCLUDE


for year in range(start_year, end_year):
    transfer_df = pd.read_sql(transfer_query, conn, params=(year - 1, year))
    transfer_dfs_dict[year] = transfer_df

for team in team_names:
    
    for year in range(start_year, end_year):
        team_roster = get_incoming_team_roster(conn, team, year) ## roster for 2021 season

        all_transfers_year = transfer_dfs_dict[year] ## get all transfers
        team_transfer_df = all_transfers_year[all_transfers_year['new_team'] == team] # get potential transfer on team
        
        num_players = len(team_roster)

        if num_players >= 6 and not team_transfer_df.empty:            
            transfers_ids = team_transfer_df['player_id'].tolist()
            for transfer_id in transfers_ids:
                avail_teams_df.loc[len(avail_teams_df)] = [team, year, transfer_id]


avail_teams_df.to_csv('Analysis/availTransferTeams.csv', index=False)
