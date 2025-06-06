library(DBI)

# Connect to your database
conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

# Your SQL query to add a column
query <- "
ALTER TABLE Team_Seasons
ADD COLUMN POSS INT;
"

# Run the query
dbExecute(conn, query)