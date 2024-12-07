import os
import json
from datetime import datetime
from typing import Optional, Union

import pandas as pd

class Cache:
    _instance: Optional["Cache"] = None
    _dirname: str = "data"

    def __new__(cls, dirname: str = "data"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._dirname = dirname
            os.makedirs(cls._dirname, exist_ok=True)

        return cls._instance

    def get(self, filepath: str) -> Optional[Union[pd.Series, pd.DataFrame]]:
        final_path = os.path.join(self._dirname, filepath)
        meta_path = os.path.join(self._dirname, "metadata.json")

        try:
            with open(meta_path, "r") as file:
                metadata = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

        expiry = metadata.get("expiries", {}).get(filepath)
        if expiry is None:
            return None

        expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
        current_date = datetime.now()

        if current_date > expiry_date:
            return None

        return pd.read_feather(final_path)

    def set(self, filepath: str, data: Union[pd.Series, pd.DataFrame], expiry: datetime):
        final_path = os.path.join(self._dirname, filepath)
        meta_path = os.path.join(self._dirname, "metadata.json")

        expiry_str = expiry.strftime("%Y-%m-%d")

        try:
            with open(meta_path, "r") as file:
                metadata = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            metadata = {"expiries": {}}

        metadata["expiries"][filepath] = expiry_str
        with open(meta_path, "w") as file:
            json.dump(metadata, file, indent=4)

        data.to_feather(final_path)
