library(DBI)
 library(BasketballAnalyzeR)
 library(dplyr)
 
 conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")
 
 features_for_cluster <- "
 This is just a comment for the feature we want: 
     - Style
         - 3prate
         - ast%
         - adjt
         - ftr
         - variance of usg%
     - Performance
         - eFG%
         - 3pts made total
         - oreb%
         - dreb%
         - aortg
         - adrtg
         - ft%
         - ast/tov
         - stl/100
 - aortg/adrtg
 - adjt
 - ftr
 - eFG%
 - 3pt rate
 - oreb%
 - dreb%
 - ast/tov
 - stl/100
 "
 
 team_name = "UCLA"
 year = 2018
 
 team_features_query <- "
 SELECT
     -- ready to go features
     team_name,
     season_year,
     adjoe,
     adjde,
     eFG AS eFG_off,
     eFG_def,
     to_percent AS to_percent_off,
     to_percent_def,
     or_percent,
     dr_percent,
     three_percent,
     three_rate,
     adjt,
     stl_pg,
     blk_pg,
     record
 FROM Team_Seasons
 WHERE season_year = ?;
 "
 
 player_features_for_team_query = "
 SELECT
     p.player_name,
     ps.season_year,
     ps.player_id,
     ps.team_name,
     ps.MIN,
     ps.FGA,
     ps.FTA,
     ps.OREB,
     ps.TOV,
     ps.adjoe,
     ps.aortg,
     ps.adrtg
 FROM Player_Seasons ps
 JOIN Players p
     ON p.player_id = ps.player_id
 "
 
 df <- dbGetQuery(conn, player_features_for_team_query)
 
 all_teams_df <- dbGetQuery(conn, team_features_query, params = list(year))
 
 
 error_minutes <- 0
 error_possessions <- 0
 error_sum <- 0
 for (i in 1:nrow(all_teams_df)) {
     team_adjoe <- all_teams_df$adjoe[i]
 
     player_team_df <- df[df$team_name ==  all_teams_df$team_name[i] & df$season_year == year, ]
 
     avg_adjoe_players_min <- weighted.mean(player_team_df$adjoe, w = player_team_df$MIN)
 
     avg_adjoe_players_pos <- weighted.mean(player_team_df$adjoe, w = player_team_df$FGA + 0.44 * player_team_df$FTA + player_team_df$TOV - player_team_df$OREB)
 
     summed_adjoe <- mean(player_team_df$adjoe)
 
     error_minutes <- error_minutes + abs(team_adjoe - avg_adjoe_players_min)
     error_possessions <- error_possessions + abs(team_adjoe - avg_adjoe_players_pos)
     error_sum <- error_sum + abs(team_adjoe - summed_adjoe)
 }
 
 
 print(error_minutes / nrow(all_teams_df))
 print(error_possessions / nrow(all_teams_df))
 print(error_sum / nrow(all_teams_df))