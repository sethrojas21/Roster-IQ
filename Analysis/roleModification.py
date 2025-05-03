from xgboost import XGBRegressor

model = XGBRegressor()
model.load_model("xgb_bpm_model.json")

print(model)