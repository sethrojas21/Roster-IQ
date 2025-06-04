library(DBI)
library(purrr)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

player_df <- dbGetQuery(conn, "
SELECT
    player_id,
    season_year,
    FGA,
    FTA,
    TOV,
    OREB
FROM Player_Seasons;
")

dbBegin(conn)

pwalk(player_df, function(player_id, season_year, FGA, FTA, TOV, OREB) {

  POSS <- as.integer(FGA + 0.44 * FTA + TOV - OREB)

  dbExecute(conn, "
    UPDATE Player_Seasons
    SET POSS = ?
    WHERE player_id = ? AND season_year = ?;
  ", params = list(POSS, player_id, season_year))
})

dbCommit(conn)