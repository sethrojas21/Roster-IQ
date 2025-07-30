library(arrow) # For cross-language file support (Feather)
library(BasketballAnalyzeR)
library(DBI)
library(dplyr)
library(cluster)
library(jsonlite)
library(readr)


# Utility: Project new data into an existing PCA space
project_to_pca <- function(newdata, pca_model) {
  # Assumes newdata is a data.frame of raw stats (not scaled)
  # pca_model: prcomp object
  # newdata columns must match those used to fit pca_model (order and names)
  # Use the scaling/centering from the PCA model
  if (!is.null(pca_model$center)) {
    newdata <- sweep(newdata, 2, pca_model$center, "-")
  }
  if (!is.null(pca_model$scale)) {
    newdata <- sweep(newdata, 2, pca_model$scale, "/")
  }
  # Project
  as.matrix(newdata) %*% pca_model$rotation
}

# Build role-specific PCA models and save artifacts
build_role_pca_models <- function(target_year, lookback_years = 3) {
  conn <- dbConnect(RSQLite::SQLite(), dbname = "rosteriq.db")
  min_year <- target_year - lookback_years
  player_features_query <- "
    SELECT
        p.player_name,
        ps.player_id,
        ps.team_name,
        ps.position,
        ps.season_year,
        ps.ts_percent,
        ps.ast_percent,
        ps.oreb_percent,
        ps.dreb_percent,
        ps.tov_percent,
        ps.ft_percent,        
        ps.stl_percent,
        ps.blk_percent,
        ps.usg_percent AS usg_rate,
        ps.ftr / 100 AS ftr,
        CASE WHEN ps.FGA != 0 THEN (ps.threeA / ps.FGA) ELSE 0.00001 END AS threeRate,
        CASE WHEN ps.FGA != 0 THEN (ps.rimA / ps.FGA) ELSE 0.00001 END AS rimRate,
        CASE WHEN ps.FGA != 0 THEN (ps.midA / ps.FGA) ELSE 0.00001 END AS midRate
    FROM Player_Seasons ps
    JOIN Players p ON ps.player_id = p.player_id
    WHERE ps.season_year < ? AND ps.season_year >= ? AND ((ps.min_pg * ps.adj_gp) > 100)
  "
  players_df <- dbGetQuery(conn, player_features_query, params = list(target_year, min_year))
  players_df <- na.omit(players_df)
  print(nrow(players_df[players_df$position == "G", ]))
  print(nrow(players_df[players_df$position == "F", ]))
  print(nrow(players_df[players_df$position == "C", ]))
  print(nrow(players_df))

  roles <- list("G", "F", "C")
  model_env <- new.env(hash = TRUE, parent = emptyenv())
  pca_data_env <- new.env(hash = TRUE, parent = emptyenv())
  labels_env <- new.env(hash = TRUE, parent = emptyenv())

  for (role in roles) {
    role_df <- players_df %>% filter(position == role)
    labels_env[[role]] <- role_df %>% select(player_name, player_id, team_name, position, season_year)
    # Subset and scale feature matrix
    df_raw <- subset(role_df, select = -c(player_name, player_id, team_name, position, season_year))
    df_scaled <- scale(df_raw)
    # Apply PCA
    pca_model <- prcomp(df_raw, center = TRUE, scale. = TRUE)
    # Save PCA parameters as JSON for Python consumption
    var_prop <- summary(pca_model)$importance[2, ]      # per‑PC variance
    cum_var  <- cumsum(var_prop)
    # num_comp <- which(cum_var >= 0.90)[1]
    num_comp <- 4
    # Store PCA-transformed data (only top num_comp)
    pca_data_env[[role]] <- as.data.frame(pca_model$x[, 1:num_comp, drop = FALSE])
    model_env[[role]] <- pca_model
    
    params_list <- list(center = pca_model$center, scale = pca_model$scale)
    param_path <- sprintf("Analysis/Clustering/Players/%s/PCA/pca_params_%s.json",
                          target_year, role)
    
    # Save the rotation matrix (loadings) as JSON
    rot_df <- as.data.frame(pca_model$rotation[, 1:num_comp, drop = FALSE])
    rot_path <- sprintf("Analysis/Clustering/Players/%s/PCA/pca_rotation_%s.json",
                        target_year, role)
    
    # Print diagnostics
    cat("\n=== PCA Summary for Role:", role, "===\n")
    print(summary(pca_model))
    cat("Keeping", num_comp, "PCs (", round(cum_var[num_comp], 3)*100, "% cumulative variance)\n")
    cat("\nLoadings (Rotation Matrix):\n")
    print(pca_model$rotation[, 1:num_comp, drop = FALSE])
    # Write Feather file for this role
    feather_path <- sprintf("Analysis/Clustering/Players/%s/PCA/pca_%s.feather", target_year, role)
    # Add player_name, position, season_year as columns for Python consumption
    feather_df <- cbind(labels_env[[role]], pca_data_env[[role]])

    loadings_df <- as.data.frame(pca_model$rotation[, 1:num_comp, drop = FALSE])
    features <- rownames(loadings_df)

    loadings_list <- lapply(features, function(f) {
      # extract the numeric vector of PC loadings for feature f
      as.numeric(loadings_df[f, ])
    })

    # give each element the proper name
    names(loadings_list) <- features
    # print(loadings_list)
    loadings_path <- sprintf("Analysis/Clustering/Players/%s/PCA/pca_loadings_%s.json", target_year, role)

    print(nrow(feather_df))
    # write pretty JSON
    write_json(
      loadings_list,
      path       = loadings_path,
      pretty     = TRUE,
      auto_unbox = TRUE
    )
    write_csv(loadings_df, loadings_path)
    write_json(rot_df, path = rot_path, rownames = "feature", pretty = TRUE)
    write_json(params_list, path = param_path, auto_unbox = TRUE, pretty = TRUE)
    write_feather(feather_df, feather_path)
  }  

  # Save RDS snapshots of all three environments
  saveRDS(model_env, file = sprintf("Analysis/Clustering/Players/%s/PCA/pca_models.rds", target_year))
  saveRDS(pca_data_env, file = sprintf("Analysis/Clustering/Players/%s/PCA/pca_data.rds", target_year))
  saveRDS(labels_env, file = sprintf("Analysis/Clustering/Players/%s/PCA/pca_labels.rds", target_year))
  dbDisconnect(conn)
  invisible(list(model_env = model_env, pca_data_env = pca_data_env, labels_env = labels_env))
}

# Example run: generate PCA artifacts for 2024 (using 2021‑2024 seasons)
for (year in 2021:2024) {
  build_role_pca_models(target_year = year, lookback_years = 3)
}