library(DBI)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")