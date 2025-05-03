library(DBI)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

asttovr_query <- "
SELECT
    player_id,
    season_year,
    ast_tov_r,
    tov_percent,
    FGA,
    FTA,
    AST,
    MIN
FROM Player_Seasons;
"

df <- dbGetQuery(conn, asttovr_query)

dbBegin(conn)
for (i in 1:nrow(df)) {
    TOV <- 0
    ast <- df$AST[i]
    tov_pct <- df$tov_percent[i]
    ast_tov <- df$ast_tov_r[i]
    fga <- df$FGA[i]
    fta <- df$FTA[i]
    min <- df$MIN[i]

    if (!is.na(ast_tov) && ast_tov > 0 && (!is.na(tov_pct) && tov_pct > 0)) {
        TOV <- ast / ast_tov
    } else if (!is.na(tov_pct) && tov_pct == 100) {
        TOV <- ceiling(min / 6)
    } else if (!is.na(tov_pct) && tov_pct > 0 && tov_pct < 100) {
        TOV <- (tov_pct / 100) * (fga + 0.44 * fta + ast) / (1 - (tov_pct / 100))
    } else {
        TOV <- 0
    }

    dbExecute(conn, "
        UPDATE Player_Seasons
        SET TOV = ?
        WHERE player_id = ?
        AND season_year = ?;
    ", params = list(as.integer(TOV), df$player_id[i], df$season_year[i]))
}

dbCommit(conn)