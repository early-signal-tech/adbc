print("📦 Loading dependencies...", flush=True)
from functions import *
from xgboost import DMatrix, train
import pyarrow.compute as pc

print("🚀 Starting model training script...", flush=True)
ducklake_files_dir = "my_ducklake.ducklake.files"

# Create ducklake table only if not already initialized
print("🔍 Checking DuckLake initialization...", flush=True)
if not is_ducklake_initialized(ducklake_files_dir):
    print(f"'{ducklake_files_dir}' folder not found or empty. Creating DuckLake table...", flush=True)
    create_penguins_ducklake()
else:
    print(f"'{ducklake_files_dir}' already exists and is not empty. Skipping DuckLake initialization.", flush=True)

# # Read ducklake as arrow table
print("📖 Reading DuckLake data as Arrow table...", flush=True)
arrow = read_penguins_ducklake()

print("Arrow table schema:")
print(arrow.schema)

# Use pyarrow to create train test sets
print("🔀 Creating train/test split...", flush=True)
arrow_train, arrow_test = get_train_test_split(arrow)

# Create DMatrix directly from arrow tables with target column
print("⚙️  Converting to XGBoost DMatrix format...", flush=True)
dtrain = DMatrix(arrow_train.drop(['species_numeric']), label=arrow_train['species_numeric'])
dtest = DMatrix(arrow_test.drop(['species_numeric']), label=arrow_test['species_numeric'])

print(f"Train dataset shape: {arrow_train.num_rows} rows")
print(f"Test dataset shape: {arrow_test.num_rows} rows")
print(f"Features: {arrow_train.drop(['species_numeric']).column_names}")
print(f"Target distribution in train set:\n{pc.value_counts(arrow_train['species_numeric'])}")
print(f"Target distribution in test set:\n{pc.value_counts(arrow_test['species_numeric'])}")

# Define XGBoost parameters
params = {
    'objective': 'multi:softmax',  # for multiclass classification
    'num_class': 3,  # species: Adelie, Chinstrap, Gentoo
    'max_depth': 6,
    'eta': 0.1,  # learning rate
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'seed': 42
}

# Train the model
print("🤖 Starting model training...", flush=True)
num_rounds = 100
evals = [(dtrain, 'train'), (dtest, 'test')]
evals_result = {}
model = train(params, dtrain, num_boost_round=num_rounds, evals=evals, evals_result=evals_result, verbose_eval=True)

# Make predictions
print("🎯 Making predictions...", flush=True)
train_preds = model.predict(dtrain)
test_preds = model.predict(dtest)

# Calculate accuracy
print("📊 Calculating accuracy...", flush=True)
train_accuracy = (train_preds == arrow_train['species_numeric'].to_pylist()).sum() / len(train_preds)
test_accuracy = (test_preds == arrow_test['species_numeric'].to_pylist()).sum() / len(test_preds)

print(f"\nModel Performance:")
print(f"Train Accuracy: {train_accuracy:.4f}")
print(f"Test Accuracy: {test_accuracy:.4f}")

# Save the model
print("💾 Saving model...", flush=True)
model.save_model('penguin_species_model.json')
print("\nModel saved to 'penguin_species_model.json'", flush=True)
print("✅ Script completed successfully!", flush=True)