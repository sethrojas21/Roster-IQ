from xgboost import XGBClassifier
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import classification_report, confusion_matrix

tier_weights = [0.8, 1, 1.2]

def train_test_class_bpm():
    # --- Two-stage BPM classification pipeline ---

    df = pd.read_csv("Analysis/PredictBPM/bpm_features_all.csv")


    # Feature columns (keep original logic)
    X = df.drop(columns=['bpm_to_predict', 'player_name', 'player_id', 'prev_year'])

    # --------- Stage 1: Binary BPM sign classifier ---------
    # y_bin: 1 if bpm_to_predict >= 0, else 0
    y_bin = (df['bpm_to_predict'] >= 0).astype(int)
    X_train_bin, X_test_bin, y_train_bin, y_test_bin = train_test_split(
        X, y_bin, test_size=0.2, random_state=42
    )

    xgb_bin = XGBClassifier(
        n_estimators=1000,
        learning_rate=0.01,
        max_depth=4,
        subsample=0.8,
        colsample_bytree=0.8,
        use_label_encoder=False,
        eval_metric='mlogloss',
        random_state=42
    )
    xgb_bin.fit(X_train_bin, y_train_bin)

    bin_preds = xgb_bin.predict(X_test_bin)
    print("\nStage 1: Binary BPM Sign Classification Report:")
    print(classification_report(y_test_bin, bin_preds))
    print("Stage 1: Confusion Matrix:")
    print(confusion_matrix(y_test_bin, bin_preds))

    # --------- Stage 2: Positive-only BPM tier split ---------
    # Only for rows with bpm_to_predict >= 0
    df_pos = df[df['bpm_to_predict'] >= 0].copy()
    # Tier: 0 for bpm 0-3, 1 for >3
    y_tier = (df_pos['bpm_to_predict'] > 3).astype(int)
    # Use the same feature columns as X, but subset rows
    X_pos = X.loc[df['bpm_to_predict'] >= 0]
    X_train_tier, X_test_tier, y_train_tier, y_test_tier = train_test_split(
        X_pos, y_tier, test_size=0.2, random_state=42
    )

    xgb_tier = XGBClassifier(
        n_estimators=1000,
        learning_rate=0.01,
        max_depth=4,
        subsample=0.8,
        colsample_bytree=0.8,
        use_label_encoder=False,
        eval_metric='mlogloss',
        random_state=42
    )
    xgb_tier.fit(X_train_tier, y_train_tier)

    tier_preds = xgb_tier.predict(X_test_tier)
    print("\nStage 2: Positive BPM Tier Classification Report (0: bpm 0-3, 1: bpm >3):")
    print(classification_report(y_test_tier, tier_preds))
    print("Stage 2: Confusion Matrix:")
    print(confusion_matrix(y_test_tier, tier_preds))

    # --------- Combine for 3-class evaluation ---------
    # 3 classes: 0 = negative bpm, 1 = 0-3, 2 = >3
    # For the test set, reconstruct predictions:
    # - If Stage 1 predicts 0: class 0
    # - If Stage 1 predicts 1 and Stage 2 predicts 0: class 1
    # - If Stage 1 predicts 1 and Stage 2 predicts 1: class 2

    # Prepare ground truth for the combined test set
    def true_3class_label(bpm):
        if bpm < 0:
            return 0
        elif 0 <= bpm <= 3:
            return 1
        else:
            return 2

    # For all X_test_bin rows, get the corresponding BPM and map to 3-class
    y_test_bpm = df.iloc[X_test_bin.index]['bpm_to_predict']
    y_test_3class = y_test_bpm.apply(true_3class_label).values

    # For predicted: need to run Stage 2 only for those predicted as positive in Stage 1
    import numpy as np
    bin_preds = np.array(bin_preds)
    stage2_indices = np.where(bin_preds == 1)[0]

    # For these, get the corresponding X_test_bin rows (which are positive-predicted)
    X_test_bin_pos = X_test_bin.iloc[stage2_indices]
    if len(X_test_bin_pos) > 0:
        tier_preds_for_bin = xgb_tier.predict(X_test_bin_pos)
    else:
        tier_preds_for_bin = np.array([])

    # Build combined prediction array
    combined_preds = np.zeros_like(bin_preds)
    for idx, pred in enumerate(bin_preds):
        if pred == 0:
            combined_preds[idx] = 0
        else:
            # Get which index in stage2_indices this is
            tier_idx = np.where(stage2_indices == idx)[0]
            if len(tier_idx) == 0:
                # Should not happen, fallback to tier 1
                combined_preds[idx] = 1
            else:
                tier_pred = tier_preds_for_bin[tier_idx[0]]
                if tier_pred == 0:
                    combined_preds[idx] = 1
                else:
                    combined_preds[idx] = 2

    print("\nCombined 3-class Confusion Matrix (0: bpm<0, 1: 0-3, 2: >3):")
    print(confusion_matrix(y_test_3class, combined_preds))
    print("Combined 3-class Classification Report:")
    print(classification_report(y_test_3class, combined_preds))

    # ----- Evaluate on transfers only -----
    # Identify transfer rows in the test split
    is_transfer_test = df.iloc[X_test_bin.index]['is_transfer'] == 1

    # Filter true and predicted labels for transfers
    y_transfers = y_test_3class[is_transfer_test.values]
    pred_transfers = combined_preds[is_transfer_test.values]

    print("\nTransfer-only 3-class Confusion Matrix:")
    print(confusion_matrix(y_transfers, pred_transfers))
    print("Transfer-only Classification Report:")
    print(classification_report(y_transfers, pred_transfers))

    xgb_bin.save_model('Analysis/PredictBPM/xgb_pos_neg_bpm_model.json')
    xgb_tier.save_model('Analysis/PredictBPM/xgb_class_tier_bpm_model.json')
# --- Reusable BPM tier probability prediction function ---
import numpy as np

def predict_bpm_tier_probs(features_df, sign_model_path='Analysis/PredictBPM/xgb_pos_neg_bpm_model.json',
                           tier_model_path='Analysis/PredictBPM/xgb_class_tier_bpm_model.json'):
    """
    Given a DataFrame of BPM features (same columns used in training),
    returns a DataFrame with columns:
      - P_neg:   probability of BPM < 0
      - P_mid:   probability of 0 <= BPM <= 3
      - P_high:  probability of BPM > 3
      - tier_pred: integer tier prediction [0,1,2]
    """
    # Load or reuse the two-stage models
    sign_model = XGBClassifier()
    sign_model.load_model(sign_model_path)
    tier_model = XGBClassifier()
    tier_model.load_model(tier_model_path)

    # Stage 1 probabilities: negative vs positive BPM
    bin_probs = sign_model.predict_proba(features_df)  # shape (n_samples, 2)
    p_neg = bin_probs[:, 0]
    p_pos = bin_probs[:, 1]

    # Stage 2 probabilities for all samples
    tier_probs = tier_model.predict_proba(features_df)  # shape (n_samples, 2)
    # Combine into three-class probabilities
    p_mid = p_pos * tier_probs[:, 0]
    p_high = p_pos * tier_probs[:, 1]

    # Build output DataFrame
    result_df = pd.DataFrame({
        'P_neg': p_neg,
        'P_mid': p_mid,
        'P_high': p_high
    }, index=features_df.index)

    # Final tier prediction: argmax of the three probabilities
    tier_map = {0: 'P_neg', 1: 'P_mid', 2: 'P_high'}
    inv_map = {v: k for k, v in tier_map.items()}
    best_cols = result_df.idxmax(axis=1)
    result_df['tier_pred'] = best_cols.map(inv_map)

    return result_df

def role_modifier(probs):
    prob_list    = [probs['P_neg'], probs['P_mid'], probs['P_high']]

    role_modifier = sum(w*p for w, p in zip(tier_weights, prob_list))

    return role_modifier