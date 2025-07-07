import time
import requests
import pandas as pd
import joblib
import logging
from sqlalchemy import create_engine, text
from email_alert import send_fraud_email

# -------------------- Configuration --------------------
API_URL = "https://upi-fraud-api-server.onrender.com/live-transactions"
MODEL_PATH = "../model/lgb_fraud_model.pkl"   
DATABASE_URI = "postgresql://postgres:Minit%402005@localhost:5432/fraud_db"
TABLE_NAME = "fraud_predictions"

# -------------------- Logging Setup --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -------------------- Load ML Model --------------------
model = joblib.load(MODEL_PATH)
logging.info("SUCCESS!\nModel loaded from %s", MODEL_PATH)

# -------------------- DB Engine --------------------
engine = create_engine(DATABASE_URI)


def preprocess_data(df):
    # One-hot encode the 'type' column
    df = pd.get_dummies(df, columns=['type'], drop_first=True)

    # Add missing columns (if not present in sample)
    for col in ['type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER']:
        if col not in df.columns:
            df[col] = 0

    # Ensure correct column order
    feature_cols = [
        'step', 'amount', 'oldbalanceOrg', 'newbalanceOrig',
        'oldbalanceDest', 'newbalanceDest',
        'type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER'
    ]
    df = df[feature_cols]
    return df


def fetch_existing_steps():
    """Fetch all unique step values already in DB."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT step FROM {TABLE_NAME}"))
        steps = {row[0] for row in result}
    return steps


def fetch_and_predict(existing_steps):
    response = requests.get(API_URL)
    if response.status_code != 200:
        logging.warning("API Error: %s", response.status_code)
        return 0

    rows = response.json().get("rows", [])
    if not rows:
        logging.info("No transactions returned from API.")
        return 0

    df_raw = pd.DataFrame(rows)

    # Drop already-seen rows
    df_raw = df_raw[~df_raw["step"].isin(existing_steps)]
    if df_raw.empty:
        logging.info("No new transactions to insert.")
        return 0

    df_processed = preprocess_data(df_raw.copy())
    predictions = model.predict(df_processed)

    df_raw['isFraud_predicted'] = predictions

    # 🔔 Send alert for each fraud row BEFORE inserting to DB
    for _, row in df_raw[df_raw["isFraud_predicted"] == 1].iterrows():
        send_fraud_email(row)

    # ⬇️ Save all predictions (including non-fraud) to DB
    df_raw.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    logging.info("Inserted %d new rows into '%s'", len(df_raw), TABLE_NAME)
    return len(df_raw)



if __name__ == "__main__":
    logging.info("Starting prediction service...")
    while True:
        try:
            steps_in_db = fetch_existing_steps()
            fetch_and_predict(steps_in_db)
        except Exception as e:
            logging.error("Unhandled error: %s", e)
        time.sleep(10)
