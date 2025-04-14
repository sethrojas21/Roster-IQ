library(DBI)

# Connect to your database
conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

# Your SQL query to add a column
add_cluster_number_query <- "
ALTER TABLE Team_Seasons
ADD COLUMN cluster INT;
"

# Run the query
dbExecute(conn, add_cluster_number_query)

# Optional: Confirm it worked
dbListFields(conn, "Team_Seasons")