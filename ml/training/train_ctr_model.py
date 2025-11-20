import os
import shutil
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, accuracy_score

import mlflow
import mlflow.sklearn
print("▶ Starting CTR training script...")
# 1. Load session-level data
SESSION_PATH = "data_lake/clickstream_refined/session_metrics"
df = pd.read_parquet(SESSION_PATH)

# Basic cleaning
df = df.dropna(subset=["device", "campaign", "referrer", "page_views", "session_converted"])
df = df[df["page_views"] > 0]

X = df[["device", "campaign", "referrer", "page_views", "session_duration_sec"]]
y = df["session_converted"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# 2. Build sklearn pipeline
cat_features = ["device", "campaign", "referrer"]
num_features = ["page_views", "session_duration_sec"]

preprocess = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat_features),
        ("num", "passthrough", num_features),
    ]
)

clf = LogisticRegression(max_iter=1000)

model = Pipeline(steps=[
    ("preprocess", preprocess),
    ("clf", clf),
])

# 3. Configure MLflow
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("clickstream_ctr_model")

with mlflow.start_run():
    print("▶ Loading data and splitting train/test...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print("▶ Training model...")
    model.fit(X_train, y_train)

    print("▶ Evaluating model...")
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    auc = roc_auc_score(y_test, y_proba)
    acc = accuracy_score(y_test, y_pred)

    print(f"   AUC = {auc:.4f}, ACC = {acc:.4f}")

    print("▶ Logging params & metrics to MLflow...")
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("max_iter", 1000)
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("accuracy", acc)

    # --- Save model locally only (no HTTP artifact upload) ---
    print("▶ Saving model locally to artifacts/ctr_model ...")
    model_dir = Path("artifacts") / "ctr_model"
    if model_dir.exists():
        shutil.rmtree(model_dir)

    mlflow.sklearn.save_model(
        sk_model=model,
        path=str(model_dir)
    )

    print(f"✅ Training complete. Model saved locally at {model_dir}")

