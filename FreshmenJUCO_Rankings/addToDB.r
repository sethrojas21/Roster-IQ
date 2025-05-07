library(DBI)

df_csv <- read.csv("FreshmenJUCO_Rankings/espnT100Recruits_2017-2025.csv")

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")


