import pandas as pd
from sklearn.preprocessing import StandardScaler
from Analysis.Helpers.dataLoader import load_players_from_multiple_clusters

column_shift = 5

def standardized_player_rate_stats(stat_query, conn, year, team_cluster_nums, player_cluster_nums, pos : str, normalized = True):
    """
    New function that deals with weights of multiple clusters
    """
    rate_stats_df = load_players_from_multiple_clusters(stat_query, 
                                                        conn, 
                                                        year, 
                                                        team_cluster_nums, 
                                                        player_cluster_nums,
                                                        pos)    
    
    columns = rate_stats_df.columns[column_shift:-2]   # Exclude meta data and team and player cluster_num (-2)

    df = rate_stats_df.copy()
    
    # scale only the rate-stat columns if requested
    scaler = None
    if normalized:
        scaler = StandardScaler().fit(df[columns])
        scaled_vals = scaler.transform(df[columns])                
        # overwrite only the rate columns with their scaled versions                   
        df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    
    
    return (df, scaler)

def scale_player_stats(
        player_stats_df: pd.DataFrame,
        scaler: StandardScaler,
        columns: list):
    # work on a copy
    df = player_stats_df.copy()
    # transform only the rate columns
    scaled_vals = scaler.transform(df[columns])
    # overwrite those columns with their z-scores
    df[columns] = pd.DataFrame(scaled_vals, columns=columns, index=df.index)
    # compute cosine similarity between the single-player row and the median vector
    player_vec = df[columns].values.reshape(1, -1)
    return player_vec

def filter_cluster_players(df, winningTeams=False, bpm=True): 
    copy_df = df.copy()
    
    if winningTeams:       
        threshold = copy_df['barthag_rank'].quantile(0.5)
        copy_df = copy_df[copy_df['barthag_rank'] <= threshold]  

    if bpm:
        copy_df = copy_df[copy_df['bpm'] > -2]
    
    return copy_df
