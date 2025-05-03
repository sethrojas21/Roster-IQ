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
    ps.FTM,
    ps.FTA,
    ps.rimA,
    ps.FGA,
    ps.FGM,
    ps.TOV,
    ps.threeM AS P3M,
    ps.threeA AS P3A,
    ps.adjoe,
    ps.adrtg
FROM Player_Seasons ps
JOIN Players p
    ON p.player_id = ps.player_id
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
- rim rate
"

players_df <- dbGetQuery(conn, player_features_for_team_query)

teams_df <- dbGetQuery(conn, team_features_query)

team_name <- teams_df$team_name
season_year <- teams_df$season_year

players_df$AST <- round(players_df$ast_pg * players_df$gp)
players_df$OREB <- round(players_df$oreb_pg * players_df$gp)
players_df$DREB <- round(players_df$dreb_pg * players_df$gp)
players_df$STL <- round(players_df$stl_pg * players_df$gp)
players_df$BLK <- round(players_df$blk_pg * players_df$gp)
players_df$MIN <- round(players_df$min_pg * players_df$gp)

# This is for teams that I know the end of year totals to make my cluster
team_stats_df <- players_df %>%
  mutate(poss = FGA + 0.44 * FTA + TOV - OREB) %>%
  group_by(team_name, season_year) %>%
  summarise(
    team_adjoe = weighted.mean(adjoe, poss, na.rm = TRUE), 
    team_adjde = weighted.mean(adrtg, poss, na.rm = TRUE),
    # team_eff_ratio = team_adjoe / team_adjde,
    # team_ast_per100 = sum(AST) / sum(poss) * 100,
    # team_stl_per100 = sum(STL) / sum(poss) * 100,
    # team_blk_per100 = sum(BLK) / sum(poss) * 100,
    team_stltov_ratio = sum(TOV) / sum(STL),
    # team_asttov_ratio = sum(AST) / sum(TOV),
    team_oreb_per100 = sum(OREB) / sum(poss) * 100,
    team_dreb_per100 = sum(DREB) / sum(poss) * 100,
    # team_ftr = sum(FTA) / sum(FGA),
    team_eFG = ( sum(FGM) * (0.5 * sum(P3M))) / sum(FGA),
    # team_rimr = sum(rimA, na.rm = TRUE) / sum(FGA),
    # team_threer = sum(P3A) / sum(FGA),
    team_3pt_fga = 3 * (sum(P3M) / sum(FGA)) *  (sum(P3A) / sum(FGA)) * (1/100)**2,
    team_adjt = first(teams_df$adjt[
        teams_df$team_name == cur_group()$team_name &
        teams_df$season_year == cur_group()$season_year
        ]),
    # team_possessions = sum(poss),
    .groups = "drop"
  )

team_labels <- paste(team_stats_df$team_name, team_stats_df$season_year, sep = " - ")

df <- scale(subset(team_stats_df, select = -c(team_name, season_year)))


# View(df)
set.seed(29)
num_clusters <- 20  # Can adjust if needed

kclu <- kclustering(
  data = df,
  k = num_clusters,
  labels = team_labels,
  nruns = 50,           # more random starts = better chance to converge
  iter.max = 500,       # much higher iteration cap
  algorithm = "Hartigan-Wong"  # default, can change to "Lloyd" if needed
)


# quartz()
# plot(kclu)
# bad_cluster_teams <- df[team_labels %in% kclu$Subjects$Label[kclu$Subjects$Cluster == 25], ]
# bad_kclu <- kclustering(bad_cluster_teams, k = 6, labels = rownames(bad_cluster_teams))
# plot(bad_kclu)

print(sapply(kclu$ClusterList, length))
profiles_df <- as.data.frame(kclu$Profiles)
write.csv(profiles_df, "kclu_profiles.csv", row.names = TRUE)
# print(kclu$Subjects %>% filter(Cluster == 25))

# Add Clusters To DB
# dbBegin(conn)

# apply(kclu$Subjects, 1, function(row) {
#   s_line <- strsplit(row["Label"], split = " - ")[[1]]
#   team <- s_line[1]
#   season <- as.integer(s_line[2])
#   cluster <- as.integer(row["Cluster"])

#   dbExecute(conn, "
#     UPDATE Team_Seasons
#     SET cluster = ?
#     WHERE team_name = ? AND season_year = ?;
#   ", params = list(cluster, team, season))
# })

# dbCommit(conn)

dbDisconnect(conn)
