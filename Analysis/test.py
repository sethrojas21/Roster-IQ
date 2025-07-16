import pandas as pd
import sqlite3
import pandas as pd
from Clustering.pcaPlayers import project_to_pca
from Clustering.matchPlayerToCluster import match_player_to_cluster_weights

player_features_query = """
SELECT
    p.player_name,
    ps.position,
    ps.season_year,
    ps.ts_percent,
    ps.ast_percent,
    ps.oreb_percent,
    ps.dreb_percent,
    ps.tov_percent,
    ps.ft_percent,        
    ps.stl_percent,
    ps.blk_percent,
    ps.usg_percent AS usg_rate,
    ps.ftr / 100 AS ftr,
    CASE WHEN ps.FGA != 0 THEN (ps.threeA / ps.FGA) ELSE 0.00001 END AS threeRate,
    CASE WHEN ps.FGA != 0 THEN (ps.rimA / ps.FGA) ELSE 0.00001 END AS rimRate,
    CASE WHEN ps.FGA != 0 THEN (ps.midA / ps.FGA) ELSE 0.00001 END AS midRate
FROM Player_Seasons ps
JOIN Players p ON ps.player_id = p.player_id
WHERE ps.player_id = ? and ps.season_year = ?
"""

df = pd.DataFrame(columns=["player_name", "cluster_weights"])
avail_transfers_df = pd.read_csv('Analysis/availTransferTeams.csv')
conn = sqlite3.connect('rosteriq.db')
cursor = conn.cursor()

for idx, row in avail_transfers_df.iterrows():
    # if idx < 40:
    #     continue
    season_year = row['season_year']
    player_id = row['player_id']
    position = cursor.execute("SELECT position FROM Player_Seasons WHERE season_year = ? AND player_id = ?",
                              (season_year, player_id)).fetchone()[0]
    player_df = pd.read_sql(player_features_query, conn, params=(player_id, season_year))

    cluster_weights_dict = match_player_to_cluster_weights(player_df[player_df.columns[3:]],
                                                        season_year,
                                                        position)
    
    print("-" * 40)
    # for k, v in cluster_weights_dict.items():
    #     print(k, v)
    # if idx > 60:
    #     exit()

