import pandas as pd
from datetime import datetime, timedelta

from iatool.core.cache import Cache

# Instantiate the Cache
cache = Cache(dirname="test_cache")

# Test data
test_data = pd.DataFrame({
    "column1": [1, 2, 3],
    "column2": ["a", "b", "c"]
})

# Filepaths
test_file = "test_data.feather"
expired_file = "expired_data.feather"

# 1. Test `set` method
expiry_date = datetime.now() + timedelta(days=1)  # Set expiry for tomorrow
cache.set(test_file, test_data, expiry_date)
print("Test data cached with expiry set for tomorrow.")

# 2. Test `get` method (valid cache)
retrieved_data = cache.get(test_file)
assert retrieved_data.equals(test_data), "Retrieved data does not match the original."
print("Test `get` method passed for valid cache.")

# 3. Test expiry handling
expired_date = datetime.now() - timedelta(days=1)  # Set expiry for yesterday
cache.set(expired_file, test_data, expired_date)
expired_retrieved = cache.get(expired_file)
assert expired_retrieved is None, "Expired data should return None."
print("Test `get` method passed for expired cache.")

# 4. Test missing file handling
missing_retrieved = cache.get("nonexistent_file.feather")
assert missing_retrieved is None, "Nonexistent file should return None."
print("Test `get` method passed for nonexistent file.")

# 5. Test metadata persistence
import json
with open("test_cache/metadata.json", "r") as meta_file:
    metadata = json.load(meta_file)
assert metadata["expiries"][test_file] == expiry_date.strftime("%Y-%m-%d"), "Metadata not updated correctly."
print("Metadata persistence test passed.")

# Cleanup (optional)
import shutil
shutil.rmtree("test_cache")  # Removes the test directory
print("Test cache directory cleaned up.")
