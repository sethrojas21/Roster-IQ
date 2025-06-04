from xgboost import XGBRegressor
import json
from Analysis.PredictBPM.queries import player_features_yearly


def cluster_params_tup():
    with open("Analysis/Clustering/scaling_params.json", "r") as f:
        scaling_params = json.load(f)
    centers = scaling_params['center']
    scale = scaling_params['scale']
    paired = list(zip(centers, scale))
    return paired

model = XGBRegressor()
model.load_model("Analysis/Predict/xgb_class_bpm_model.json")
pairs = cluster_params_tup()


# def test_2022_2023():
#     players_2022 = 