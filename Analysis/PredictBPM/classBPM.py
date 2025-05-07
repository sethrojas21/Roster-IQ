from xgboost import XGBClassifier
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error


df = pd.read_csv("Analysis/PredictBPM/bpm_features.csv")

# Classification: BPM Tier Prediction
def bpm_to_tier(bpm):
    if bpm < 0:
        return 0
    elif bpm < 3:
        return 1
    else:
        return 2

df['bpm_tier'] = df['bpm_to_predict'].apply(bpm_to_tier)

X_cls = df.drop(columns=['bpm_to_predict', 'player_name', 'player_id', 'prev_year', 'bpm_tier'])
y_cls = df['bpm_tier']
X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(X_cls, y_cls, test_size=0.2, random_state=42)

classifier = XGBClassifier(
    n_estimators=1000,
    learning_rate=0.01,
    max_depth=4,
    subsample=0.8,
    colsample_bytree=0.8,
    use_label_encoder=False,
    eval_metric='mlogloss',
    random_state=42
)
classifier.fit(X_train_cls, y_train_cls)

from sklearn.metrics import classification_report, confusion_matrix

cls_preds = classifier.predict(X_test_cls)
print("\nClassification Report (BPM Tiers):")
print(classification_report(y_test_cls, cls_preds))

print("Confusion Matrix:")
print(confusion_matrix(y_test_cls, cls_preds))

# classifier.save_model("Analysis/PredictBPM/xgb_class_bpm_model.json")