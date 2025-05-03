library(DBI)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

query <- "
SELECT 
    p.player_name,
    ps.MIN,
    ps.AST,
    ps.DREB,
    ps.OREB,
    ps.STL,
    ps.TOV,
    ps.BLK
FROM Player_Seasons ps
JOIN Players p
    ON ps.player_id = p.player_id
LIMIT 10;
"

df <- dbGetQuery(conn, query)
View(df)