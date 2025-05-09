library(DBI)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

create_tables <- function() {
  # Create table for ESPN HS Recruits
  dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS HS_Rankings (
        player_name TEXT,
        position TEXT,
        height_inches INTEGER,
        ranking INTEGER,
        season_year INTEGER,
        school_committed TEXT
    );
    ")

  # Create table for JUCO Recruits
  dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS JUCO_Rankings (
        player_name TEXT,
        position TEXT,
        height_inches INTEGER,
        ranking INTEGER,
        season_year INTEGER,
        school_committed TEXT
    );
    ")
}

# Add hs rankings
hs_df <- read.csv("FreshmenJUCO_Rankings/espnT100Recruits_2017-2025.csv")
hs_df <- subset(hs_df, select = -ignore)
View(hs_df)

dbBegin(conn)
# Insert each row into the HS_Rankings table
for (i in 1:nrow(hs_df)) {

    school_val <- hs_df$school_committed[i]
    if (is.null(school_val) || is.na(school_val) || school_val == "") {
    school_val <- NULL
    }
  dbExecute(conn, "
    INSERT INTO HS_Rankings (player_name, position, height_inches, ranking, season_year, school_committed)
    VALUES (?, ?, ?, ?, ?, ?);
  ", params = list(
    hs_df$player_name[i],
    hs_df$position[i],
    hs_df$height_inches[i],
    hs_df$ranking[i],
    hs_df$season_year[i],
    school_val
  ))
}
dbCommit(conn)
dbDisconnect(conn)