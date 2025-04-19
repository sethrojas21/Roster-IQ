library(DBI)

# Connect to your database
conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

# Your SQL query to add a column
add_cluster_number_query <- "
ALTER TABLE Player_Seasons
ADD COLUMN adj_gp INT;
"

# Run the query
dbExecute(conn, add_cluster_number_query)