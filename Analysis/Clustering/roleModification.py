from xgboost import XGBRegressor

model = XGBRegressor()
model.load_model("Analysis/xgb_bpm_model.json")

