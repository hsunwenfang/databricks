"""
Fraud Detection — Inference Pipeline (Local Runnable)

Two paths (both use the model saved by train.py):
  A. Batch inference  — score all new transactions, write predictions CSV
  B. Simulated real-time — score one transaction at a time (simulates REST endpoint)

Run train.py first to generate data and model.
"""

import json
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Config (mirrors train.py)
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
GOLD_DIR = DATA_DIR / "gold"
MODEL_DIR = PROJECT_DIR / "model"
PREDICTIONS_DIR = DATA_DIR / "predictions"
PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

MODEL_PATH = MODEL_DIR / "fraud_model.joblib"
META_PATH = MODEL_DIR / "model_meta.json"
FRAUD_THRESHOLD = 0.7


# ===================================================================
# Load model + metadata
# ===================================================================
def load_model():
    """Load the champion model saved by train.py."""
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. Run train.py first."
        )
    model = joblib.load(MODEL_PATH)
    meta = json.loads(META_PATH.read_text())
    print(f"[LOAD] Model: {meta['model_name']} v{meta['version']} (alias: {meta['alias']})")
    print(f"[LOAD] ROC AUC at training: {meta['roc_auc']}")
    return model, meta


# ===================================================================
# PATH A: Batch Inference
# ===================================================================
def batch_inference(model, meta: dict):
    """
    Score all transactions in the Gold feature table.
    Simulates a Streaming Table reading new rows and scoring with UDF.
    """
    features_path = GOLD_DIR / "transaction_features.csv"
    if not features_path.exists():
        raise FileNotFoundError(f"Features not found: {features_path}. Run train.py first.")

    gold = pd.read_csv(features_path)
    feature_cols = meta["feature_cols"]

    print(f"\n{'='*60}")
    print("PATH A: BATCH INFERENCE")
    print(f"{'='*60}")
    print(f"[BATCH] Scoring {len(gold)} transactions...")

    X = gold[feature_cols]
    gold["fraud_probability"] = model.predict_proba(X)[:, 1].round(4)
    gold["is_fraud_predicted"] = (gold["fraud_probability"] >= FRAUD_THRESHOLD).astype(int)
    gold["model_version"] = f"v{meta['version']}"
    gold["scored_at"] = datetime.now().isoformat()

    # Write predictions
    predictions = gold[[
        "transaction_id", "user_id", "amount",
        "fraud_probability", "is_fraud_predicted",
        "is_fraud",  # ground truth for evaluation
        "model_version", "scored_at",
    ]]
    out = PREDICTIONS_DIR / "batch_predictions.csv"
    predictions.to_csv(out, index=False)
    print(f"[BATCH] Predictions → {out}")

    # Summary
    total = len(predictions)
    flagged = predictions["is_fraud_predicted"].sum()
    actual = predictions["is_fraud"].sum()
    print(f"\n[BATCH] Summary:")
    print(f"  Total scored:       {total}")
    print(f"  Flagged as fraud:   {flagged} ({100*flagged/total:.1f}%)")
    print(f"  Actual fraud:       {actual} ({100*actual/total:.1f}%)")

    # Confusion-style breakdown
    tp = ((predictions["is_fraud_predicted"] == 1) & (predictions["is_fraud"] == 1)).sum()
    fp = ((predictions["is_fraud_predicted"] == 1) & (predictions["is_fraud"] == 0)).sum()
    fn = ((predictions["is_fraud_predicted"] == 0) & (predictions["is_fraud"] == 1)).sum()
    tn = ((predictions["is_fraud_predicted"] == 0) & (predictions["is_fraud"] == 0)).sum()
    print(f"  TP={tp}, FP={fp}, FN={fn}, TN={tn}")
    if tp + fp > 0:
        print(f"  Precision: {tp/(tp+fp):.2%}")
    if tp + fn > 0:
        print(f"  Recall:    {tp/(tp+fn):.2%}")

    # Generate alerts
    alerts = predictions[predictions["fraud_probability"] >= FRAUD_THRESHOLD].copy()
    alerts["alert_severity"] = pd.cut(
        alerts["fraud_probability"],
        bins=[0, 0.85, 0.95, 1.01],
        labels=["MEDIUM", "HIGH", "CRITICAL"],
    )
    alerts_out = PREDICTIONS_DIR / "fraud_alerts.csv"
    alerts.to_csv(alerts_out, index=False)
    print(f"\n[ALERTS] {len(alerts)} alerts → {alerts_out}")
    if not alerts.empty:
        print(alerts[["transaction_id", "amount", "fraud_probability", "alert_severity"]].to_string(index=False))

    return predictions


# ===================================================================
# PATH B: Simulated Real-Time Inference
# ===================================================================
def realtime_inference(model, meta: dict):
    """
    Simulate real-time scoring of individual transactions.
    In Databricks, this would be a Mosaic AI Serving Endpoint (REST API).
    """
    feature_cols = meta["feature_cols"]

    print(f"\n{'='*60}")
    print("PATH B: REAL-TIME INFERENCE (simulated)")
    print(f"{'='*60}")

    # Simulated incoming transactions (as if from a payment gateway)
    test_transactions = [
        {
            "name": "Normal purchase",
            "amount": 45.0, "hour_of_day": 14, "day_of_week": 2,
            "is_weekend": 0, "avg_amount_user": 50.0, "max_amount_user": 120.0,
            "txn_count_user": 8.0, "amount_zscore": -0.2, "is_cross_border": 0,
        },
        {
            "name": "Suspicious: high amount, 3am, cross-border",
            "amount": 4500.0, "hour_of_day": 3, "day_of_week": 1,
            "is_weekend": 0, "avg_amount_user": 50.0, "max_amount_user": 120.0,
            "txn_count_user": 8.0, "amount_zscore": 12.5, "is_cross_border": 1,
        },
        {
            "name": "Weekend splurge (legit high spend)",
            "amount": 350.0, "hour_of_day": 20, "day_of_week": 5,
            "is_weekend": 1, "avg_amount_user": 200.0, "max_amount_user": 400.0,
            "txn_count_user": 15.0, "amount_zscore": 1.1, "is_cross_border": 0,
        },
    ]

    for txn in test_transactions:
        name = txn.pop("name")
        X = pd.DataFrame([txn])[feature_cols]
        proba = model.predict_proba(X)[0, 1]
        decision = "BLOCK" if proba >= FRAUD_THRESHOLD else "ALLOW"

        print(f"\n  Transaction: {name}")
        print(f"    Amount: ${txn['amount']:.2f} | Hour: {txn['hour_of_day']} | Cross-border: {txn['is_cross_border']}")
        print(f"    Fraud probability: {proba:.4f}")
        print(f"    Decision: {decision}")


# ===================================================================
# PATH C: Model Monitoring (Drift Check)
# ===================================================================
def check_drift(predictions: pd.DataFrame):
    """
    Simple drift detection: compare prediction distribution
    against expected baseline. In Databricks, this is Lakehouse Monitoring.
    """
    print(f"\n{'='*60}")
    print("PATH C: MONITORING (drift check)")
    print(f"{'='*60}")

    # Baseline stats (from training)
    baseline_fraud_rate = 0.08  # expected ~8%
    baseline_avg_score = 0.15   # expected average fraud probability

    # Current stats
    current_fraud_rate = predictions["is_fraud_predicted"].mean()
    current_avg_score = predictions["fraud_probability"].mean()

    # Simple drift detection (in production: KS test, chi-squared, PSI)
    fraud_rate_drift = abs(current_fraud_rate - baseline_fraud_rate) / baseline_fraud_rate
    score_drift = abs(current_avg_score - baseline_avg_score) / baseline_avg_score

    print(f"  Fraud rate — baseline: {baseline_fraud_rate:.2%}, current: {current_fraud_rate:.2%}, drift: {fraud_rate_drift:.2%}")
    print(f"  Avg score  — baseline: {baseline_avg_score:.2f}, current: {current_avg_score:.4f}, drift: {score_drift:.2%}")

    drift_threshold = 0.30  # 30% relative change
    if fraud_rate_drift > drift_threshold or score_drift > drift_threshold:
        print(f"\n  ⚠  DRIFT DETECTED (threshold: {drift_threshold:.0%})")
        print(f"  → Action: trigger retrain workflow (train.py)")
    else:
        print(f"\n  ✓ No significant drift (threshold: {drift_threshold:.0%})")

    # Save monitoring report
    report = {
        "checked_at": datetime.now().isoformat(),
        "n_predictions": len(predictions),
        "baseline_fraud_rate": baseline_fraud_rate,
        "current_fraud_rate": round(current_fraud_rate, 4),
        "fraud_rate_drift": round(fraud_rate_drift, 4),
        "baseline_avg_score": baseline_avg_score,
        "current_avg_score": round(current_avg_score, 4),
        "score_drift": round(score_drift, 4),
        "drift_detected": bool(fraud_rate_drift > drift_threshold or score_drift > drift_threshold),
    }
    report_path = PREDICTIONS_DIR / "monitoring_report.json"
    report_path.write_text(json.dumps(report, indent=2))
    print(f"\n  Report → {report_path}")


# ===================================================================
# Main
# ===================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("FRAUD DETECTION — INFERENCE PIPELINE")
    print("=" * 60)

    model, meta = load_model()

    # Path A: batch scoring
    predictions = batch_inference(model, meta)

    # Path B: simulated real-time
    realtime_inference(model, meta)

    # Path C: monitoring
    check_drift(predictions)

    print(f"\n{'='*60}")
    print("INFERENCE PIPELINE COMPLETE")
    print(f"  Predictions: {PREDICTIONS_DIR / 'batch_predictions.csv'}")
    print(f"  Alerts:      {PREDICTIONS_DIR / 'fraud_alerts.csv'}")
    print(f"  Monitor:     {PREDICTIONS_DIR / 'monitoring_report.json'}")
    print("=" * 60)
