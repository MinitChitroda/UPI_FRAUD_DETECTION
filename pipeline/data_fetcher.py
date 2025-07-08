import asyncio
import pandas as pd
import requests
import joblib
import logging
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from pipeline.email_alert import send_fraud_email
from dotenv import load_dotenv
import os

# -------------------- Init --------------------
load_dotenv()
app = FastAPI()

# -------------------- Config --------------------
API_URL = "https://upi-fraud-api-server.onrender.com/live-transactions"
MODEL_PATH = "../model/lgb_fraud_model.pkl"
DATABASE_URI = os.getenv("DATABASE_URI")
TABLE_NAME = "fraud_predictions"

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -------------------- Model & Engine --------------------
model = joblib.load(MODEL_PATH)
logging.info("Success!!\nModel loaded from %s", MODEL_PATH)
engine = create_engine(DATABASE_URI)


def preprocess_data(df):
    df = pd.get_dummies(df, columns=['type'], drop_first=True)
    for col in ['type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER']:
        if col not in df.columns:
            df[col] = 0
    feature_cols = [
        'step', 'amount', 'oldbalanceOrg', 'newbalanceOrig',
        'oldbalanceDest', 'newbalanceDest',
        'type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER'
    ]
    return df[feature_cols]


def fetch_existing_steps():
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT step FROM {TABLE_NAME}"))
        return {row[0] for row in result}


def fetch_and_predict(existing_steps):
    response = requests.get(API_URL)
    if response.status_code != 200:
        logging.warning("API Error: %s", response.status_code)
        return 0

    rows = response.json().get("rows", [])
    if not rows:
        logging.info("No transactions from API.")
        return 0

    df_raw = pd.DataFrame(rows)
    df_raw = df_raw[~df_raw["step"].isin(existing_steps)]
    if df_raw.empty:
        logging.info("No new rows.")
        return 0

    df_processed = preprocess_data(df_raw.copy())
    predictions = model.predict(df_processed)
    df_raw['isFraud_predicted'] = predictions

    for _, row in df_raw[df_raw["isFraud_predicted"] == 1].iterrows():
        send_fraud_email(row)

    df_raw.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    logging.info("Inserted %d new predictions", len(df_raw))
    return len(df_raw)


# -------------------- Async Background Task --------------------
async def pipeline_loop():
    logging.info("🚀 Background fraud detection loop started.")
    while True:
        try:
            steps_in_db = fetch_existing_steps()
            fetch_and_predict(steps_in_db)
        except Exception as e:
            logging.error("Error: %s", e)
        await asyncio.sleep(10)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(pipeline_loop())


@app.get("/")
def root():
    return {"status": "UPI Fraud Detection Pipeline running ✅"}
