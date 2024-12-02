from abc import abstractmethod
from typing import Self

import pandas as pd
import aiohttp

from ..data.fmp import fetch_data_fmp

class Data:
    def __init__(self):
        self._data = None
    
    @property
    def data(self) -> dict:
        return self._data
    
    @classmethod
    @abstractmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        pass

    @abstractmethod
    async def update(self, session: aiohttp.ClientSession):
        pass

class CompanyProfileData(Data):
    def __init__(self, ticker: str):
        super().__init__()
        self._ticker = ticker

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        data = cls(ticker)
        await data.update(session)

        return data
    
    async def update(self, session: aiohttp.ClientSession):
        raw_data = await fetch_data_fmp(session, f"/profile/{self._ticker}")
        raw_data = raw_data[0]

        key_mapping = {
            "symbol": "ticker",
            "price": "price",
            "beta": "beta",
            "volAvg": "vol_avg",
            "mktCap": "market_cap",
            "lastDiv": "last_dividend",
            "range": "price_range",
            "changes": "price_changes",
            "companyName": "name",
            "currency": "currency",
            "cik": "cik",
            "isin": "isin",
            "cusip": "cusip",
            "exchange": "exchange_name",
            "exchangeShortName": "exchange_ticker",
            "industry": "industry",
            "website": "website",
            "description": "description",
            "ceo": "ceo",
            "sector": "sector",
            "country": "country",
            "fullTimeEmployees": "num_full_time_employees",
            "phone": "phone",
            "address": "address",
            "city": "city",
            "state": "state",
            "zip": "zip",
            "dcfDiff": "dcf_diff",
            "dcf": "dcf",
            "image": "image_url",
            "ipoDate": "ipo_date",
            "defaultImage": "image_default",
            "isEtf": "is_etf",
            "isActivelyTrading": "is_actively_trading",
            "isAdr": "is_adr",
            "isFund": "is_fund",
        }

        mapped_data = {key_mapping.get(k, k): v for k, v in raw_data.items()}

        self._data = pd.Series(mapped_data)

