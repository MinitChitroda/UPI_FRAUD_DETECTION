import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix

# Step 1: Connect to PostgreSQL database
engine = create_engine("postgresql://postgres:Minit%402005@localhost:5432/fraud_db")

# Step 2: Load predicted data stored in the DB (predictions made by the deployed model)
print("Loading predicted data from PostgreSQL...")
db_df = pd.read_sql("SELECT * FROM fraud_predictions", con=engine)
print(f"✅ Loaded {len(db_df)} predicted rows from database.")

# Step 3: Load original dataset with ground truth labels (for cross-checking)
print("Loading original dataset with labels...")
original_df = pd.read_csv("upi_data_150k.csv")
print(f"✅ Loaded {len(original_df)} rows from CSV.")

# Step 4: Merge the datasets on unique transaction keys
merge_cols = [
    "step", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig", "nameDest"
]
print("Merging prediction data with ground truth labels...")
merged_df = pd.merge(db_df, original_df, on=merge_cols, how="inner")
print(f"Merged {len(merged_df)} rows for comparison.")

# Step 5: Compare model predictions vs actual labels
y_true = merged_df['isFraud']
y_pred = merged_df['isFraud_predicted']

print("\n===== Evaluation Metrics =====")
print(f"Accuracy: {accuracy_score(y_true, y_pred):.6f}")
print("\nClassification Report:\n", classification_report(y_true, y_pred))
print("\nConfusion Matrix:\n", confusion_matrix(y_true, y_pred))

# Optional: Save misclassified rows to CSV
wrong_preds = merged_df[merged_df['isFraud'] != merged_df['isFraud_predicted']]
wrong_preds.to_csv("misclassified_transactions.csv", index=False)
print(f"\nSaved {len(wrong_preds)} misclassified transactions to 'misclassified_transactions.csv'")