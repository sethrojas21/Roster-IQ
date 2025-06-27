library(DBI)
library(BasketballAnalyzeR)
library(dplyr)
library(jsonlite)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

for (year in 2021:2024) {
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
  WHERE season_year < ?
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
      ps.PTS,
      ps.threeM AS P3M,
      ps.threeA AS P3A,
      ps.adjoe,
      ps.adrtg,
      ps.POSS
  FROM Player_Seasons ps
  JOIN Players p
      ON p.player_id = ps.player_id
  WHERE ps.season_year < ?;
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

  players_df <- dbGetQuery(conn, player_features_for_team_query, params = list(year))

  teams_df <- dbGetQuery(conn, team_features_query, params = list(year))

  team_name <- teams_df$team_name
  season_year <- teams_df$season_year

  players_df$AST <- round(players_df$ast_pg * players_df$gp)
  players_df$OREB <- round(players_df$oreb_pg * players_df$gp)
  players_df$DREB <- round(players_df$dreb_pg * players_df$gp)
  players_df$STL <- round(players_df$stl_pg * players_df$gp)
  players_df$BLK <- round(players_df$blk_pg * players_df$gp)
  players_df$MIN <- round(players_df$min_pg * players_df$gp)

  # This is for teams that I know the end of year totals to make my cluster
  aggregate_team_stats_from_players_df <- function(players_df) {
    team_stats_df <- players_df %>%
    mutate(poss = FGA + 0.44 * FTA + TOV - OREB) %>%
    group_by(team_name, season_year) %>%
    summarise(
      adjoe = weighted.mean(adjoe, poss, na.rm = TRUE),
      adjde = weighted.mean(adrtg, poss, na.rm = TRUE),
      stltov = sum(TOV) / sum(STL),    
      oreb100 = sum(OREB) / sum(poss) * 100,
      dreb100 = sum(DREB) / sum(poss) * 100,    
      eFG = ( sum(FGM) + (0.5 * sum(P3M))) / sum(FGA),        
      .groups = "drop"
    )
  }

  team_stats_df <- aggregate_team_stats_from_players_df(players_df)
  team_labels <- paste(team_stats_df$team_name, team_stats_df$season_year, sep = " - ")

  df <- scale(subset(team_stats_df, select = -c(team_name, season_year)))

  set.seed(29)
  kclu <- kclustering(
    data = df,
    k = 10,
    labels = team_labels,
    nruns = 50,           # more random starts = better chance to converge
    iter.max = 100,       # much higher iteration cap
    algorithm = "Hartigan-Wong"  # default, can change to "Lloyd" if needed
  )

  plot_clusters <- function() {
    quartz()
    plot(kclu)
  }  

  # Add Clusters To DB
  save_cluster_to_db <- function() {
      dbBegin(conn)

      apply(kclu$Subjects, 1, function(row) {
        s_line <- strsplit(row["Label"], split = " - ")[[1]]
        team <- s_line[1]
        season <- as.integer(s_line[2])
        cluster <- as.integer(row["Cluster"])

        dbExecute(conn, "
          UPDATE Team_Seasons
          SET cluster = ?
          WHERE team_name = ? AND season_year = ?;
        ", params = list(cluster, team, season))
      })      
      dbCommit(conn)
  }

  save_cluster_to_df <- function() {
    # Convert table to proper data frame with named columns
    subject_df <- as.data.frame(kclu$Subjects, stringsAsFactors = FALSE)
    colnames(subject_df) <- c("Label", "Cluster")

    # Build new data frame
    row_list <- lapply(1:nrow(subject_df), function(i) {
      label <- subject_df[i, "Label"]
      cluster <- subject_df[i, "Cluster"]
      
      s_line <- strsplit(label, split = " - ")[[1]]
      if (length(s_line) != 2) {
        warning(paste("Skipping bad label format:", label))
        return(NULL)
      }

      team <- s_line[1]
      season <- as.integer(s_line[2])
      cluster <- as.integer(cluster)

      data.frame(team_name = team, season_year = season, cluster_num = cluster, stringsAsFactors = FALSE)
    })

    # Remove any NULLs (from bad rows) and combine
    row_list <- Filter(Negate(is.null), row_list)
    df <- do.call(rbind, row_list)    
    return(df)
  }

  save_cluster_info <- function() {
      profiles_df <- as.data.frame(kclu$Profiles)
      csv_filepath <- sprintf("Analysis/Clustering/ClusterData/%.0f/kclu_profiles.csv", year)
      write.csv(profiles_df, csv_filepath, row.names = FALSE)

      scale_center <- attr(df, "scaled:center")
      scale_scale <- attr(df, "scaled:scale")
      scale_info <- list(center = scale_center, scale = scale_scale)
      json_filepath <- sprintf("Analysis/Clustering/ClusterData/%.0f/scaling_params.json", year)
      jsonlite::write_json(scale_info, json_filepath, pretty = TRUE, auto_unbox = TRUE)

      df <- save_cluster_to_df()
      df_csv_filepath <- sprintf("Analysis/Clustering/ClusterData/%.0f/teamSeasonClusterLabel.csv", year)
      write.csv(df, df_csv_filepath, row.names = FALSE)
  }

  save_cluster_info()

}

dbDisconnect(conn)
