library(DBI)

# Connect to your database
conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

# Your SQL query to add a column
query <- "ALTER TABLE HS_Rankings ADD COLUMN %s %s;"

dbExecute(conn, sprintf(query, "FTA", "INT"))

# float_columns_to_add <- c("bpm", "adjoe", "adjde")
# int_columns_to_add <- c("TOV", "STL", "OREB", "DREB", "FGM", "P3M", "FGA", "MIN")

# for (column in float_columns_to_add) {
#     # Run the query
#     dbExecute(conn, sprintf(query, column, "FLOAT"))
# }

# for (column in int_columns_to_add) {
#     # Run the query
#     dbExecute(conn, sprintf(query, column, "INT"))
# }

