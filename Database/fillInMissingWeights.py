import pandas as pd
import numpy as np
import sqlite3
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.linear_model import Ridge, Lasso
from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import GradientBoostingRegressor

# Connect to the SQLite database
conn = sqlite3.connect("rosteriq.db")

year = 2022

# Query to get all players with non-null weights
query = """
SELECT ps.height_inches, ps.position, ps.weight_lbs, ts.team_name, ts.conf AS conference,
       ps.efg_percent, ps.ts_percent, ps.usg_percent, ps.oreb_percent, ps.dreb_percent, ps.ast_percent,
       ps.tov_percent, ps.ft_percent, ps.ftr, ps.twoA, ps.two_percent, ps.threeA, ps.three_percent,
       ps.blk_percent, ps.stl_percent, ps.pfr, ps.ast_tov_r, ps.rimA, ps.rimshot_percent,
       ps.midA, ps.midshot_percent, ps.dunksA, ps.dunksshot_percent
FROM Player_Seasons ps
JOIN Team_Seasons ts ON ps.team_name = ts.team_name
WHERE ts.season_year = ? AND ps.season_year = ? AND ps.weight_lbs IS NOT NULL AND ps.height_inches IS NOT NULL
"""
data = pd.read_sql(query, conn, params=(year, year))

# Ensure the dataset contains the required columns
required_columns = {'height_inches', 'position', 'weight_lbs', 'efg_percent', 'ts_percent', 'usg_percent',
                    'oreb_percent', 'dreb_percent', 'ast_percent', 'tov_percent', 'ft_percent', 'ftr',
                    'twoA', 'two_percent', 'threeA', 'three_percent', 'blk_percent', 'stl_percent',
                    'pfr', 'ast_tov_r', 'rimA', 'rimshot_percent', 'midA', 'midshot_percent', 'dunksA', 'dunksshot_percent'}
if data.empty or not required_columns.issubset(data.columns):
    print("Skipping due to missing data.")
    exit()

# Check for missing values in the original dataset
print("Missing values in the original dataset:")
print(data.isnull().sum())

# Drop rows with missing values or handle them
data = data.dropna()  # Alternatively, use data.fillna(data.mean(), inplace=True)

# Define features and target
X = data[['height_inches', 'position', 'efg_percent', 'ts_percent', 'usg_percent', 'oreb_percent',
          'dreb_percent', 'ast_percent', 'tov_percent', 'ft_percent', 'ftr', 'twoA', 'two_percent',
          'threeA', 'three_percent', 'blk_percent', 'stl_percent', 'pfr', 'ast_tov_r', 'rimA',
          'rimshot_percent', 'midA', 'midshot_percent', 'dunksA', 'dunksshot_percent']]
y = data['weight_lbs']

# One-hot encode the position column with handle_unknown='ignore'
encoder = OneHotEncoder(drop='first', sparse_output=False, handle_unknown='ignore')

# Fit the encoder on the training data
position_encoded = encoder.fit_transform(X[['position']])

# Check for missing values after encoding
print("Missing values after encoding:")
print(pd.DataFrame(position_encoded).isnull().sum())

# Create feature array for training
X_encoded = np.hstack((X.drop(columns=['position']).values, position_encoded))

# Split into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X_encoded, y, test_size=0.2, random_state=42)

# Scale the numerical features (excluding one-hot encoded columns)
scaler = StandardScaler()
X_train[:, :len(X.columns) - 1] = scaler.fit_transform(X_train[:, :len(X.columns) - 1])
X_test[:, :len(X.columns) - 1] = scaler.transform(X_test[:, :len(X.columns) - 1])

# Check for missing values after scaling
print("Missing values after scaling:")
print(pd.DataFrame(X_train).isnull().sum())
print(pd.DataFrame(X_test).isnull().sum())

# Feature selection using Lasso
feature_selector = SelectFromModel(Lasso(alpha=0.01, random_state=42))
feature_selector.fit(X_train, y_train)

# Transform the training and test sets to keep only selected features
X_train_selected = feature_selector.transform(X_train)
X_test_selected = feature_selector.transform(X_test)

# Train a Ridge Regression model
ridge_model = Ridge(alpha=1.0, random_state=42)
ridge_model.fit(X_train_selected, y_train)

# Predict and evaluate Ridge Regression
y_pred_ridge = ridge_model.predict(X_test_selected)
mae_ridge = mean_absolute_error(y_test, y_pred_ridge)
r2_ridge = r2_score(y_test, y_pred_ridge)

print("Ridge Regression Results:")
print(f"MAE: {mae_ridge:.2f}")
print(f"R² Score: {r2_ridge:.2f}")
print("-" * 40)

# Train a Gradient Boosting Regressor
gb_model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=3, random_state=42)
gb_model.fit(X_train_selected, y_train)

# Predict and evaluate Gradient Boosting
y_pred_gb = gb_model.predict(X_test_selected)
mae_gb = mean_absolute_error(y_test, y_pred_gb)
r2_gb = r2_score(y_test, y_pred_gb)

print("Gradient Boosting Results:")
print(f"MAE: {mae_gb:.2f}")
print(f"R² Score: {r2_gb:.2f}")
print("-" * 40)

# Train an XGBoost regression model
model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=6, random_state=42)
model.fit(X_train_selected, y_train)

# Predict and evaluate
y_pred = model.predict(X_test_selected)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"XGBoost Results:")
print(f"MAE: {mae:.2f}")
print(f"R² Score: {r2:.2f}")
print("-" * 40)

# Query players with null weights
null_weight_query = """
SELECT ps.height_inches, ps.position, ts.team_name, p.player_name, ts.conf AS conference,
       ps.efg_percent, ps.ts_percent, ps.usg_percent, ps.oreb_percent, ps.dreb_percent, ps.ast_percent,
       ps.tov_percent, ps.ft_percent, ps.ftr, ps.twoA, ps.two_percent, ps.threeA, ps.three_percent,
       ps.blk_percent, ps.stl_percent, ps.pfr, ps.ast_tov_r, ps.rimA, ps.rimshot_percent,
       ps.midA, ps.midshot_percent, ps.dunksA, ps.dunksshot_percent
FROM Player_Seasons ps
JOIN Team_Seasons ts ON ps.team_name = ts.team_name
JOIN Players p ON ps.player_id = p.player_id
WHERE ts.season_year = ? AND ps.season_year = ? AND ps.weight_lbs IS NULL AND ps.height_inches IS NOT NULL
"""
null_weight_data = pd.read_sql(null_weight_query, conn, params=(year, year))

if null_weight_data.empty:
    print("No players with null weights.")
    exit()

# Handle missing values in the null weight query
print("Missing values in null_weight_data:")
print(null_weight_data.isnull().sum())
null_weight_data = null_weight_data.dropna()

# Prepare features for prediction
X_null = null_weight_data[['height_inches', 'position', 'efg_percent', 'ts_percent', 'usg_percent',
                           'oreb_percent', 'dreb_percent', 'ast_percent', 'tov_percent', 'ft_percent',
                           'ftr', 'twoA', 'two_percent', 'threeA', 'three_percent', 'blk_percent',
                           'stl_percent', 'pfr', 'ast_tov_r', 'rimA', 'rimshot_percent', 'midA',
                           'midshot_percent', 'dunksA', 'dunksshot_percent']]

# Ensure that only known categories are used in the position column
known_positions = encoder.categories_[0]
X_null = X_null[X_null['position'].isin(known_positions)]

if X_null.empty:
    print("No valid players with known positions.")
    exit()

# Transform the position column using the fitted encoder
position_null_encoded = encoder.transform(X_null[['position']])

# Create the feature array for prediction
X_null_encoded = np.hstack((X_null.drop(columns=['position']).values, position_null_encoded))

# Scale the numerical features
X_null_encoded[:, :len(X_null.columns) - 1] = scaler.transform(X_null_encoded[:, :len(X_null.columns) - 1])

# Check for NaN values in the final prediction array
print("Missing values in X_null_encoded:")
print(pd.DataFrame(X_null_encoded).isnull().sum())

# Predict weights for players with null weights
predicted_weights = model.predict(X_null_encoded)

# Add predictions to the DataFrame
null_weight_data = null_weight_data[null_weight_data['position'].isin(known_positions)]
null_weight_data['predicted_weight_lbs'] = predicted_weights

# Print the results, including player_name
print("Predicted weights for players:")
print(null_weight_data[['team_name', 'player_name', 'height_inches', 'position', 'predicted_weight_lbs']])
print("-" * 40)