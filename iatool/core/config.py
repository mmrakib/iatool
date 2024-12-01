import json

from typing import Any, Dict, Self, Optional

class Config:
    _instance: Optional["Config"] = None
    _data: Dict[str, Any] = {}

    def __new__(cls, filepath: str = "config.json") -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)

            with open(filepath, "r") as file:
                cls._instance._data = json.load(file)

            for section, values in cls._instance._data.items():
                setattr(cls._instance, section, ConfigSection(values))

        return cls._instance

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(f"'Config' object has no attribute '{name}'")

class ConfigSection:
    def __init__(self, values: Dict[str, Any]):
        for key, value in values.items():
            setattr(self, key, value)

_ = Config()
