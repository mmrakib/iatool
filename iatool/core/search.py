from typing import List, Optional, Self

import aiohttp

from .data import AllTickersExchangeData

class SearchTool:
    _instance: Optional[Self] = None
    _session: Optional[aiohttp.ClientSession] = None

    def __new__(cls, session: aiohttp.ClientSession = None) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._session = session

        return cls._instance
    
    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise ValueError("HTTP session does not exist for search tool")

        return self._session
    
    @session.setter
    def session(self, session: aiohttp.ClientSession):
        self._session = session

    async def get_all_tickers_exchange(self, exchange: str) -> List[str]:
        if self._session is None:
            raise ValueError("HTTP session does not exist for search tool")

        exchange_tickers = await AllTickersExchangeData.create(self._session, exchange)
        
        return exchange_tickers.data
