from Analysis.Helpers.dataLoader import get_incoming_synthetic_roster
from Analysis.SyntheticRosters.aggregateRosterStats import aggregate_team_stats_from_players_df
from Analysis.Clustering.matchTeamToCluster import match_team_to_cluster_weights, match_team_cluster_to_label
from Analysis.Clustering.matchPlayerToCluster import get_player_stats, match_player_to_cluster_weights, match_player_cluster_to_label
from Analysis.Helpers.standardization import get_nPercentile_scalar_and_vals
import numpy as np


class InitBenchmarkPlayer:
            
    def __init__(self, conn, team_name, incoming_season_year, player_id_to_replace):
        """
        Stores variables to run other scripts
        """
        # Meta
        self.conn = conn
        self.team_name = team_name
        self.season_year = incoming_season_year
        self.replaced_plyr_id = player_id_to_replace
        self.team_k = 1
        self.player_k = 1
        max_k = 2

        # Team Stuff
        player_stats_df, _ = get_incoming_synthetic_roster(conn, team_name, incoming_season_year, player_id_to_replace)
        aggregated_team_stats = aggregate_team_stats_from_players_df(player_stats_df)
        self.team_clusterID_weights_dict = match_team_to_cluster_weights(aggregated_team_stats,
                                                                   incoming_season_year,
                                                                   k=self.team_k)
        self.team_ids = list(self.team_clusterID_weights_dict.keys())
        self.team_weights = list(self.team_clusterID_weights_dict.values())
        # self.team_labels = match_team_cluster_to_label(incoming_season_year,
        #                                           self.team_ids)
        self.team_labels = []
        
        # Player stuff
        self.replaced_plyr_stats = get_player_stats(player_id_to_replace, incoming_season_year, conn)    
        self.replaced_plyr_pos = self.replaced_plyr_stats['position']
        self.plyr_clusterID_weights_dict = match_player_to_cluster_weights(self.replaced_plyr_stats,
                                                                      incoming_season_year,
                                                                      self.replaced_plyr_pos,
                                                                      k=self.player_k,
                                                                      team_id=self.team_ids[0])
        self.plyr_ids = list(self.plyr_clusterID_weights_dict.keys())
        self.plyr_weights = list(self.plyr_clusterID_weights_dict.values())
        # self.plyr_labels = match_player_cluster_to_label(incoming_season_year,
        #                                                  self.replaced_plyr_pos,
        #                                                  self.plyr_ids)
        self.plyr_labels = []

        self.fs_benchmark_dict_saved = None   
        self.vocbp_benchmark_dict_saved = None
        self.length = 0


    def fs_query():
        return """
        -- ps.efg_percent,
        -- ps.ast_percent,
        -- ps.oreb_percent,
        -- ps.dreb_percent,
        -- ps.tov_percent,
        -- ps.ft_percent,        
        -- ps.stl_percent,
        -- ps.blk_percent,
        ps.usg_percent,
        (ps.threeA / ps.FGA) AS threeRate,
        (ps.ast_pg * ps.adj_gp) / ps.FGA AS ast_fga,
        ps.FGA / ps.adj_gp AS fg_pg,
        ps.ftr,
        (ps.rimA / ps.FGA) AS rimRate,
        (ps.midA / ps.FGA) AS midRate
    """

    def vocbp_query():
        return """
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

    def effective_sample_size(weights, lengths):
        # Expand weights so each player in the cluster gets equal share of its cluster weight
        expanded_weights = []
        for w, l in zip(weights, lengths):
            if l > 0:
                expanded_weights.extend([w / l] * l)
        expanded_weights = np.array(expanded_weights)
        
        if expanded_weights.sum() == 0:
            return 0
        
        return (expanded_weights.sum() ** 2) / (expanded_weights ** 2).sum()
        

    def fs_benchmark(self, adaptive_k = True):
        if self.fs_benchmark_dict_saved:
            return self.fs_benchmark_dict_saved
        
        scalar, bmark_vals, lth = get_nPercentile_scalar_and_vals(InitBenchmarkPlayer.fs_query(), 
                                                             self.conn, 
                                                             self.season_year,                                                              
                                                             self.team_clusterID_weights_dict,
                                                             self.plyr_clusterID_weights_dict,
                                                             self.replaced_plyr_pos)

        dict = {
            "scalar" : scalar,
            "vals" : bmark_vals
        }
        self.fs_benchmark_dict_saved = dict
        self.length = lth
        return dict
    
    def fs_scalar(self):
        if self.fs_benchmark_dict_saved:
            return self.fs_benchmark_dict_saved['scalar']
        
        return self.fs_benchmark()['scalar']
        
    def fs_bmark_vals(self):
        if self.fs_benchmark_dict_saved:
            return self.fs_benchmark_dict_saved['vals']
        
        return self.fs_benchmark()['vals']

    def vocbp_benchmark(self):
        if self.vocbp_benchmark_dict_saved:
            return self.vocbp_benchmark_dict_saved

        scalar, bmark_vals, lth = get_nPercentile_scalar_and_vals(InitBenchmarkPlayer.vocbp_query(), 
                                                             self.conn, 
                                                             self.season_year,                                                              
                                                             self.team_clusterID_weights_dict,
                                                             self.plyr_clusterID_weights_dict,
                                                             self.replaced_plyr_pos)

        dict = {
            "scalar" : scalar,
            "vals" : bmark_vals
        }

        self.vocbp_benchmark_dict_saved = dict
        self.length = lth
        return dict
    
    def vocbp_scalar(self):
        if self.vocbp_benchmark_dict_saved:
            return self.vocbp_benchmark_dict_saved['scalar']
        
        return self.vocbp_benchmark()['scalar']
        
    def vocbp_bmark_vals(self):
        if self.vocbp_benchmark_dict_saved:
            return self.vocbp_benchmark_dict_saved['vals']
        
        return self.vocbp_benchmark()['vals']
