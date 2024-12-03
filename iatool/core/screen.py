from typing import List, Optional, Self, Callable

from .asset import Asset

class ScreenTool:
    _instance: Optional[Self] = None

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance
    
    def run(self, assets: List[Asset], predicate: Callable[[Asset], bool]) -> List[Asset]:
        filtered_assets = []

        for asset in assets:
            if predicate(asset):
                filtered_assets.append(asset)

        return filtered_assets
