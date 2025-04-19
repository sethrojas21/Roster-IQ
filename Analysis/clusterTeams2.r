library(DBI)
library(BasketballAnalyzeR)
library(dplyr)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")


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
    games_played AS total_gp
FROM Team_Seasons
GROUP BY team_name, season_year;
"

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
- aortg/adrtg (use weighted mean on possessions) (weighted.mean(players$adjoe, w = players$FGA + 0.44 * players$FTA + players$TOV))
- adjt
- ftr
- eFG%
- 3pt rate
- oreb%
- dreb%
- ast/100
- tov/100
- stl/100
- blk/100
- rim/mid/dunk rate
"

player_features_for_team_query = "
SELECT
    p.player_name,
    ps.season_year,
    ps.player_id,
    ps.team_name,
    ps.games_played,
    ps.adj_gp AS gp,
    ps.pts_pg,
    ps.min_pg,
    ps.ast_pg,
    ps.oreb_pg,
    ps.dreb_pg,
    ps.stl_pg,
    ps.blk_pg,
    ps.OREB AS orb,
    ps.FTM,
    ps.FTA,
    ps.FGA,
    ps.FGM,
    ps.threeM,
    ps.adjoe,
    ps.adrtg
FROM Player_Seasons ps
JOIN Players p
    ON p.player_id = ps.player_id
"

players_df <- dbGetQuery(conn, player_features_for_team_query)

teams_df <- dbGetQuery(conn, team_features_query)

attach(players_df)

team_name <- teams_df$team_name
season_year <- teams_df$season_year

players_df$AST <- as.integer(ast_pg * adjusted_games_played)
players_df$OREB <- round(oreb_pg * adjusted_games_played)
players_df$DREB <- as.integer(dreb_pg * adjusted_games_played)
players_df$STL <- round((stl_pg * adjusted_games_played))
players_df$BLK <- as.integer(blk_pg * adjusted_games_played)
players_df$MIN <- min_pg * adjusted_games_played


abil_players <- players_df[players_df$season_year == 2023 & players_df$team_name == "Hartford", ]

abil_players$oreb_total <- abil_players$oreb_pg * abil_players$games_played

print(abil_players)
print(sum(abil_players$oreb_total))
print(sum(abil_players$OREB))


# team_stats_df <- players_df %>%
#   mutate(poss = FGA + 0.44 * FTA + TOV - OREB) %>%
#   group_by(team_name, season_year) %>%
#   summarise(
#     team_adjoe = weighted.mean(adjoe, poss, na.rm = TRUE), 
#     team_adjde = weighted.mean(adrtg, poss, na.rm = TRUE),
#     team_ast_per100 = sum(AST) / sum(poss) * 100,
#     team_stl_per100 = sum(STL) / sum(poss) * 100,
#     team_blk_per100 = sum(BLK) / sum(poss) * 100,
#     team_oreb_per100 = sum(OREB) / sum(poss) * 100,
#     team_dreb_per100 = sum(DREB) / sum(poss) * 100,
#     total_oreb = sum(OREB),
#     total_orb = sum(orb),
#     total_stl = sum(STL),

#     .groups = "drop"
#   )

# detach(players_df)

# adjoe <- team_stats_df$team_adjoe

# adjde <- team_stats_df$team_adjde

# View(team_stats_df)

dbDisconnect(conn)

