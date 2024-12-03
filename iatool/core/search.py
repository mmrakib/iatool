from typing import List, Optional, Self

from .data import AllTickersExchangeData

class SearchTool:
    _instance: Optional[Self] = None

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance
    
    async def get_all_tickers_exchange(self, exchange: str) -> List[str]:
        exchange_tickers = await AllTickersExchangeData.create(exchange)
        
        return exchange_tickers.data
