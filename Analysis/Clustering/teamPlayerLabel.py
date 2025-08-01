import sqlite3
import pandas as pd
from Analysis.config import Config


columns = ['player_name', 'season_year', 'Cluster', 'player_id', 'team_name', 'team_cluster']
df = pd.DataFrame(columns=columns)


conn = sqlite3.connect('rosteriq.db')
all_year_df = pd.DataFrame(columns=columns)
for year in range(Config.START_YEAR, Config.END_YEAR_EXCLUDE):
    year_df = pd.DataFrame(columns=columns)

    poses_df = pd.DataFrame(columns=columns[:-1]) # don't include team clsuter
    for pos in Config.POSITIONS:
        pos_df = pd.read_csv(f'Analysis/Clustering/Players/{year}/KClustering/player_labels_{pos}.csv')
        poses_df = pd.concat([poses_df, pos_df], ignore_index=True)
    
    team_df = pd.read_csv(f'Analysis/Clustering/15ClusterData/{year}/KClustering/labels.csv')

    merged_df = pd.merge(poses_df, team_df, how='left', on=['team_name', 'season_year'])
    all_year_df = pd.concat([all_year_df, merged_df])
    

all_year_df = all_year_df.rename(columns={'Cluster' : 'player_cluster'})
all_year_df.to_csv('Analysis/Clustering/teamPlayerLabel.csv', index=False)
