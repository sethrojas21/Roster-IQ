library(DBI)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

all_players_query = "
SELECT
    player_id,
    season_year,
    stl_pg,
    games_played,
    blk_pg
FROM Player_Seasons
"

all_players_df = dbGetQuery(conn, all_players_query)

for(i in 1:nrow(all_players_df)) {
    STL <-  as.integer(all_players_df$stl_pg[i] * all_players_df$games_played[i])
    BLK <- as.integer(all_players_df$blk_pg[i] * all_players_df$games_played[i])

    player_id <- all_players_df$player_id[i]
    season_year <- all_players_df$season_year[i]

    dbExecute(conn, "
    UPDATE Player_Seasons
    SET STL = ?, BLK = ?
    WHERE player_id = ? AND season_year = ?;
    ", params = list(STL, BLK, player_id, season_year))
}
