library(DBI)
library(BasketballAnalyzeR)
library(dplyr)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

# Select data from team seasons
queryFF <- "
SELECT 
    team_name,
    season_year,
    adjoe,
    adjde,
    eFG AS eFG_off,
    eFG_def,
    to_percent AS to_percent_off,
    to_percent_def,
    or_percent,
    dr_percent,
    three_percent,
    three_rate,
    adjt
FROM Team_Seasons
--WHERE season_year = 2023
GROUP BY team_name, season_year;
"

FF.DF <- dbGetQuery(conn, queryFF)

attach(FF.DF)
TeamName <- team_name

SeasonYear <- season_year 

OD.Rtg <- adjoe / adjde

OD.eFG.F1 <- eFG_off / eFG_def

OD.TOV.F2 <-  to_percent_off / to_percent_def

O.REB.F3 <- or_percent

D.REB.F3 <- dr_percent

P3PFGA <- 3 * three_percent * three_rate * (1/100)**2 # Expected 3p points per field goal attempted

ADJT <- adjt
detach(FF.DF)

df <- data.frame(TeamName, SeasonYear, OD.Rtg, OD.eFG.F1, OD.TOV.F2, O.REB.F3, D.REB.F3, P3PFGA, ADJT)

FF.stats <- df[c("OD.Rtg", "OD.eFG.F1", "OD.TOV.F2", "O.REB.F3", "D.REB.F3", "P3PFGA", "ADJT")]

FF.stats <- scale(FF.stats)

set.seed(20)

kclu1 <- kclustering(
  data = FF.stats,
  k = 20, # optimal
  labels = df$TeamName,
  nclumax = 30,         # max clusters to try if k = NULL (ignored here)
  nruns = 50,           # more random starts = better chance to converge
  iter.max = 500,       # much higher iteration cap
  algorithm = "Hartigan-Wong"  # default, can change to "Lloyd" if needed
)
quartz()
plot(kclu1)

# Extract cluster assignments and join with metadata
clusters <- kclu1$Subjects
clusters$season_year <- df$SeasonYear
clusters <- clusters %>%
  rename(team_name = Label, team_cluster = Cluster)

# agg <- aggregate(FF.stats, by = list(cluster = clusters$cluster), mean)
cluster_sizes <- as.data.frame(table(kclu1$Subjects$Cluster))
colnames(cluster_sizes) <- c("cluster", "team_count")
View(clusters)
View(cluster_sizes)

# Add cluster numbers to the database
readline(prompt="You sure?")
for (i in 1:nrow(clusters)) {
  query <- sprintf(
    "UPDATE Team_Seasons
     SET cluster = %d
     WHERE team_name = '%s' AND season_year = %d;",
    clusters$team_cluster[i],
    gsub("'", "''", clusters$team_name[i]),  # escape single quotes
    clusters$season_year[i]
  )
  dbExecute(conn, query)
}
dbDisconnect()


### Find optimal number of clusters by minimizing Cluster Heterogeneity Index (CHI)
# chis <- c()
# avg_cluster_sizes <- c()
# k_values <- 2:30

# nonconverged_k <- c()  # store k's that triggered warnings

# for (k in k_values) {
#   print(paste("Clustering with k =", k))
  
#   result <- tryCatch({
#     kclu_temp <- kclustering(FF.stats, labels = df$TeamName, k = k, nruns = 50, iter.max = 500)
#     chis <- c(chis, kclu_temp$CHI)
#     cluster_counts <- table(kclu_temp$Subjects$Cluster)
#     avg_cluster_sizes <- c(avg_cluster_sizes, mean(cluster_counts))
#   }, warning = function(w) {
#     message(sprintf("⚠️ Warning at k = %d: %s", k, conditionMessage(w)))
#     nonconverged_k <<- c(nonconverged_k, k)
#     chis <<- c(chis, NA)
#     avg_cluster_sizes <<- c(avg_cluster_sizes, NA)
#   })
# }

# print("Non-converged k values:")
# print(nonconverged_k)

# Optional: Plot average cluster sizes
# quartz()
# plot(k_values, avg_cluster_sizes, type = "b", xlab = "Number of Clusters", ylab = "Average Cluster Size", main = "Average Cluster Size by k")
### END
