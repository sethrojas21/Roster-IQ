library(DBI)

conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")

team_gp_query = "
SELECT 
    team_name,
    season_year,
    games_played AS total_gp
FROM Team_Seasons
GROUP BY team_name, season_year
"

players_gp_query = "
SELECT
    p.player_name,
    ps.player_id,
    ps.season_year,
    ps.ast_pg,
    ps.team_name,
    ps.games_played
FROM Player_Seasons ps
JOIN Players p
    ON ps.player_id = p.player_id;
"

team_gp_df <- dbGetQuery(conn, team_gp_query)
player_gp_df <- dbGetQuery(conn, players_gp_query)

teams_under_total_gp <- data.frame(
    team_name = character(),
    gp_difference = numeric(),
    season_year = numeric(),
    stringsAsFactors = FALSE
)

player_gp_df$adjusted_games_played <- player_gp_df$games_played
for (i in 1:nrow(team_gp_df)) {
    team_name <- team_gp_df$team_name[i]
    season_year <- team_gp_df$season_year[i]
    total_gp <- team_gp_df$total_gp[i]

    players_on_team <- player_gp_df[player_gp_df$team_name == team_name & player_gp_df$season_year == season_year, ]
    max_gp_players <- max(players_on_team$games_played)

    idx <- player_gp_df$team_name == team_name & 
        player_gp_df$season_year == season_year & 
        player_gp_df$player_id %in% players_on_team$player_id

    difference <- total_gp - max_gp_players
    player_gp_df$adjusted_games_played[idx] <- player_gp_df$games_played[idx] + difference
}

for (i in 1:nrow(player_gp_df)) {
    dbExecute(conn, "
    UPDATE Player_Seasons
    SET adj_gp = ?
    WHERE player_id = ? 
    AND season_year = ?;
    ", params = list(player_gp_df$adjusted_games_played[i], player_gp_df$player_id[i], player_gp_df$season_year[i]))
}