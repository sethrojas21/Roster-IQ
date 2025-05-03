library(DBI)
library(purrr)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

player_df <- dbGetQuery(conn, "
SELECT
    player_id,
    season_year,
    adj_gp AS gp,
    min_pg,
    ast_pg,
    oreb_pg,
    dreb_pg,
    treb_pg,
    stl_pg,
    blk_pg
FROM Player_Seasons;
")

dbBegin(conn)

pwalk(player_df, function(player_id, season_year, gp, min_pg, ast_pg, oreb_pg,
                          dreb_pg, treb_pg, stl_pg, blk_pg) {

  MIN  <- as.integer(min_pg  * gp)
  AST  <- as.integer(ast_pg  * gp)
  OREB <- as.integer(oreb_pg * gp)
  DREB <- as.integer(dreb_pg * gp)
  TREB <- as.integer(treb_pg * gp)
  STL  <- as.integer(stl_pg  * gp)
  BLK  <- as.integer(blk_pg  * gp)

  dbExecute(conn, "
    UPDATE Player_Seasons
    SET MIN = ?, AST = ?, OREB = ?, DREB = ?, TREB = ?, STL = ?, BLK = ?
    WHERE player_id = ? AND season_year = ?;
  ", params = list(MIN, AST, OREB, DREB, TREB, STL, BLK, player_id, season_year))
})

dbCommit(conn)