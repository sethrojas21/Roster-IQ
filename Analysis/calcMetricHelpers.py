from Clustering.matchPlayerToCluster import get_player_stats, match_player_to_cluster_weights
from Clustering.matchTeamToCluster import match_team_to_cluster_weights
from SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df
from dataLoader import get_incoming_synthetic_roster, get_transfers
from standardization import get_nPercentile_scalar_and_vals_roles, get_nPercentile_scalar_and_vals

def calc_score_data_helper(query_snippet, conn, team_name, incoming_season_year, player_id_to_replace):
    synthetic_team_df, _ = get_incoming_synthetic_roster(conn, team_name, incoming_season_year, player_id_to_replace)    
    # Get team stats and match them to a cluster
    synthethic_team_stats = aggregate_team_stats_from_players_df(synthetic_team_df)
    closest_team_cluster_weights = match_team_to_cluster_weights(synthethic_team_stats, incoming_season_year)     
    # Get benchmark player info
    replaced_plyr_stats = get_player_stats(player_id_to_replace, incoming_season_year, conn)    
    replaced_plyr_pos = replaced_plyr_stats['position']    
    closest_player_cluster_weights = match_player_to_cluster_weights(replaced_plyr_stats[3:],
                                                               incoming_season_year,
                                                               replaced_plyr_pos)
    
    transfer_data = get_transfers(conn, incoming_season_year, replaced_plyr_pos, query_snippet)
    
    # get scaler and median values
    scalar, med_vals = get_nPercentile_scalar_and_vals(query_snippet, 
                                                             conn, 
                                                             incoming_season_year,                                                              
                                                             closest_team_cluster_weights,
                                                             closest_player_cluster_weights,
                                                             replaced_plyr_pos)
    
    return scalar, med_vals, transfer_data

vocbp_query_snippet = """
        -- (ps.AST / ps.POSS) * 100 AS ast100,
        -- (ps.OREB / ps.POSS) * 100 AS oreb100,
        -- (ps.DREB / ps.POSS) * 100 AS dreb100,
        -- (CAST(ps.STL AS REAL) * 100 / ps.POSS) AS stl100,
        --(CAST(ps.BLK AS REAL) * 100 / ps.POSS) AS blk100,        
        -- ps.tov_percent,
        ps.ast_percent,
        ps.oreb_percent,
        ps.dreb_percent,
        ps.ft_percent,        
        ps.stl_percent,
        ps.blk_percent,
        ps.ts_percent
    """

fs_query_snippet = """
        -- ps.efg_percent,
        -- ps.ast_percent,
        -- ps.oreb_percent,
        -- ps.dreb_percent,
        -- ps.tov_percent,
        -- ps.ft_percent,        
        -- ps.stl_percent,
        --ps.blk_percent,
        ps.usg_percent,
        (ps.threeA / ps.FGA) AS threeRate,
        (ps.ast_pg * ps.adj_gp) / ps.FGA AS ast_fga,
        ps.FGA / ps.adj_gp AS fg_pg,
        ps.ftr,
        (ps.rimA / ps.FGA) AS rimRate,
        (ps.midA / ps.FGA) AS midRate
    """