from xgboost import XGBRegressor
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error


df = pd.read_csv("Analysis/PredictBPM/bpm_features.csv")

X_names = df['player_name']
X_ids = df['player_id']
X_years = df['prev_year']
# Train/test split
X = df.drop(columns=['bpm_to_predict', 'player_name', 'player_id','prev_year'])
y = df['bpm_to_predict']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = XGBRegressor(
    n_estimators=1000,
    learning_rate=0.01,
    max_depth=4,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42
)
model.fit(X_train, y_train)

# Evaluate
preds = model.predict(X_test)
print("Predictions vs Actual BPM:")
for actual, pred, name in zip(y_test, preds, X_names[X_test.index]):
    print(f"Player: {name}, Actual: {actual:.2f}, Predicted: {pred:.2f}")
    
mae = mean_absolute_error(y_test, preds)
print(f"MAE on minimal feature set: {mae:.2f}")
print("R^2", model.score(X_test, y_test))