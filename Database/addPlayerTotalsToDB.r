library(DBI)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

player_df <- dbGetQuery(conn, "
SELECT
    player_id,
    adj_gp AS gp,
    min_pg,
    ast_pg,
    oreb_pg,
    dreb_pg,
    treb_pg,
    stl_pg,
    stl_pg,
    blk_pg
FROM Player_Seasons;
")

for (i in 1:nrow(player_df)) {
    
}

