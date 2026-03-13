"""
Fraud Detection — Training Pipeline (Local Runnable)

Simulates the Databricks Lakehouse pattern locally:
  Bronze (raw) → Silver (cleaned) → Gold (features) → Model

Uses: pandas, scikit-learn, mlflow (all local, no Spark/Databricks needed)
"""

import os
import json
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
MODEL_DIR = PROJECT_DIR / "model"

for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, MODEL_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ===================================================================
# 1. BRONZE — Generate & ingest raw synthetic transactions
# ===================================================================
def generate_bronze(n_rows: int = 200, seed: int = 42) -> pd.DataFrame:
    """Simulate raw transaction data landing in the Bronze layer."""
    rng = np.random.default_rng(seed)

    users = [f"user_{i:03d}" for i in range(20)]
    merchants = [f"merchant_{i:03d}" for i in range(10)]
    countries = ["JP", "US", "GB", "SG"]

    base_time = datetime(2026, 3, 1)
    timestamps = [base_time + timedelta(hours=int(h)) for h in rng.integers(0, 240, n_rows)]

    df = pd.DataFrame({
        "transaction_id": [f"txn_{i:05d}" for i in range(n_rows)],
        "user_id": rng.choice(users, n_rows),
        "merchant_id": rng.choice(merchants, n_rows),
        "amount": np.round(rng.exponential(scale=80, size=n_rows), 2),
        "country": rng.choice(countries, n_rows),
        "hour_of_day": [t.hour for t in timestamps],
        "day_of_week": [t.weekday() for t in timestamps],
        "timestamp": timestamps,
    })

    # Inject ~8% fraud: high amount + odd hour + foreign country
    fraud_mask = rng.random(n_rows) < 0.08
    df.loc[fraud_mask, "amount"] = np.round(rng.uniform(800, 5000, fraud_mask.sum()), 2)
    df.loc[fraud_mask, "hour_of_day"] = rng.choice([1, 2, 3, 4], fraud_mask.sum())
    df.loc[fraud_mask, "country"] = rng.choice(["NG", "RU", "BR"], fraud_mask.sum())
    df["is_fraud"] = fraud_mask.astype(int)

    # Inject a few bad rows (nulls / negative amounts) for quality checks
    bad_idx = rng.choice(df.index, size=5, replace=False)
    df.loc[bad_idx[:2], "amount"] = -1.0
    df.loc[bad_idx[2:4], "user_id"] = None
    df.loc[bad_idx[4:], "merchant_id"] = None

    out = BRONZE_DIR / "raw_transactions.csv"
    df.to_csv(out, index=False)
    print(f"[BRONZE] Generated {len(df)} rows → {out}")
    return df


# ===================================================================
# 2. SILVER — Clean, validate, deduplicate
# ===================================================================
def clean_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Data quality checks + cleaning (simulates DLT EXPECT)."""
    total = len(bronze_df)

    # Quality checks
    valid_mask = (
        (bronze_df["amount"] > 0)
        & bronze_df["user_id"].notna()
        & bronze_df["merchant_id"].notna()
    )
    passed = valid_mask.sum()
    print(f"[SILVER] Quality: {passed}/{total} rows passed ({100*passed/total:.1f}%)")

    silver = bronze_df[valid_mask].copy()
    silver = silver.drop_duplicates(subset=["transaction_id"])
    silver["amount"] = silver["amount"].astype(float)

    out = SILVER_DIR / "clean_transactions.csv"
    silver.to_csv(out, index=False)
    print(f"[SILVER] Cleaned {len(silver)} rows → {out}")
    return silver


# ===================================================================
# 3. GOLD — Feature engineering
# ===================================================================
def build_features(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Window-based features per user (simulates Spark window functions)."""
    silver_df = silver_df.sort_values(["user_id", "timestamp"])

    # Per-user rolling stats (last 5 transactions as proxy for 7d window)
    grouped = silver_df.groupby("user_id")["amount"]
    silver_df["avg_amount_user"] = grouped.transform(lambda x: x.rolling(5, min_periods=1).mean())
    silver_df["max_amount_user"] = grouped.transform(lambda x: x.rolling(5, min_periods=1).max())
    silver_df["txn_count_user"] = grouped.transform(lambda x: x.rolling(5, min_periods=1).count())

    # Z-score: how far this txn deviates from user's average
    std = grouped.transform(lambda x: x.rolling(5, min_periods=1).std()).fillna(1.0).replace(0, 1.0)
    silver_df["amount_zscore"] = (silver_df["amount"] - silver_df["avg_amount_user"]) / std

    # Cross-border (user's most common country = "home")
    home = silver_df.groupby("user_id")["country"].agg(lambda x: x.mode().iloc[0])
    silver_df["home_country"] = silver_df["user_id"].map(home)
    silver_df["is_cross_border"] = (silver_df["country"] != silver_df["home_country"]).astype(int)

    # Weekend flag
    silver_df["is_weekend"] = silver_df["day_of_week"].isin([5, 6]).astype(int)

    # Select final feature set
    feature_cols = [
        "transaction_id", "user_id", "amount",
        "hour_of_day", "day_of_week", "is_weekend",
        "avg_amount_user", "max_amount_user", "txn_count_user",
        "amount_zscore", "is_cross_border", "is_fraud",
    ]
    gold = silver_df[feature_cols].copy()

    out = GOLD_DIR / "transaction_features.csv"
    gold.to_csv(out, index=False)
    print(f"[GOLD]   Engineered {len(gold)} rows, {len(feature_cols)-2} features → {out}")
    return gold


# ===================================================================
# 4. TRAIN — GradientBoosting + MLflow tracking
# ===================================================================
FEATURE_COLS = [
    "amount", "hour_of_day", "day_of_week", "is_weekend",
    "avg_amount_user", "max_amount_user", "txn_count_user",
    "amount_zscore", "is_cross_border",
]


def train_model(gold_df: pd.DataFrame):
    """Train, evaluate, and save model."""
    X = gold_df[FEATURE_COLS]
    y = gold_df["is_fraud"]

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )
    print(f"\n[TRAIN] Train: {len(X_train)}, Val: {len(X_val)}")
    print(f"[TRAIN] Fraud rate — train: {y_train.mean():.2%}, val: {y_val.mean():.2%}")

    params = {
        "n_estimators": 100,
        "max_depth": 4,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "random_state": 42,
    }

    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    # Evaluate
    y_proba = model.predict_proba(X_val)[:, 1]
    y_pred = model.predict(X_val)
    roc_auc = roc_auc_score(y_val, y_proba)

    print(f"\n[TRAIN] ROC AUC: {roc_auc:.4f}")
    print(f"\n{classification_report(y_val, y_pred, target_names=['legit', 'fraud'])}")

    # Feature importance
    importance = pd.DataFrame({
        "feature": FEATURE_COLS,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    print("[TRAIN] Feature importance:")
    print(importance.to_string(index=False))

    # Save model with joblib
    model_path = MODEL_DIR / "fraud_model.joblib"
    joblib.dump(model, model_path)
    print(f"\n[TRAIN] Model saved → {model_path}")

    # Save metadata (simulates UC Model Registry alias)
    meta = {
        "model_name": "fraud_detection_model",
        "alias": "champion",
        "version": 1,
        "roc_auc": round(roc_auc, 4),
        "params": params,
        "feature_cols": FEATURE_COLS,
        "trained_at": datetime.now().isoformat(),
    }
    meta_path = MODEL_DIR / "model_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f"[TRAIN] Metadata  → {meta_path}")

    return model, roc_auc


# ===================================================================
# Main
# ===================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("FRAUD DETECTION — TRAINING PIPELINE")
    print("=" * 60)

    bronze_df = generate_bronze(n_rows=200)
    silver_df = clean_silver(bronze_df)
    gold_df = build_features(silver_df)
    model, auc_score = train_model(gold_df)

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print(f"  Bronze: {BRONZE_DIR / 'raw_transactions.csv'}")
    print(f"  Silver: {SILVER_DIR / 'clean_transactions.csv'}")
    print(f"  Gold:   {GOLD_DIR / 'transaction_features.csv'}")
    print(f"  Model:  {MODEL_DIR / 'fraud_model'}")
    print(f"  AUC:    {auc_score:.4f}")
    print("=" * 60)
