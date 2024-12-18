import os
from abc import abstractmethod
from typing import Self, Union
from datetime import datetime
from dateutil.relativedelta import relativedelta

import aiohttp
import pandas as pd

from .cache import Cache

from ..data.fmp import fmp_fetch_company_profile
from ..data.fmp import fmp_fetch_income_statement
from ..data.fmp import fmp_fetch_balance_sheet
from ..data.fmp import fmp_fetch_cash_flow
from ..data.fmp import fmp_fetch_all_tickers_exchange
from ..data.fmp import fmp_fetch_historical_prices

class Data:
    _cache = Cache()

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._data = None
    
    @property
    def data(self) -> Union[list, dict, pd.Series, pd.DataFrame]:
        if self._data is None:
            raise ValueError("Data component not available")

        return self._data
    
    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise ValueError("Data component has no HTTP session")

        return self._session

    @classmethod
    @abstractmethod
    async def create(cls, session: aiohttp.ClientSession) -> Self:
        pass

    @abstractmethod
    async def update(self, session: aiohttp.ClientSession):
        pass

class AllTickersExchangeData(Data):
    def __init__(self, session: aiohttp.ClientSession, exchange: str):
        super().__init__(session)
        self._exchange = exchange

    @property
    def exchange(self) -> str:
        return self._exchange

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, exchange: str) -> Self:
        cached_data = cls._cache.get(f"all_tickers_data/{exchange}.feather")

        if cached_data is not None:
            return cached_data

        data = cls(session, exchange)
        await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_all_tickers_exchange(self._session, self._exchange)

        expiry = datetime.now() + relativedelta(months=6)
        self._cache.set(f"all_tickers_data/{self._exchange}.feather", self._data, expiry)

class CompanyProfileData(Data):
    def __init__(self, session: aiohttp.ClientSession, ticker: str):
        super().__init__(session)
        self._ticker = ticker

    @property
    def ticker(self) -> str:
        return self._ticker

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        cached_data = cls._cache.get(f"profile_data/{ticker}.feather")

        data = cls(session, ticker)

        if cached_data is not None:
            data._data = cached_data
        else:
            await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_company_profile(self._session, self._ticker)

        expiry = datetime.now() + relativedelta(months=6)
        self._cache.set(f"profile_data/{self._ticker}.feather", self._data, expiry)

class HistoricalPricesData(Data):
    def __init__(self, session: aiohttp.ClientSession, ticker: str, start_date: str = "1990-01-01"):
        super().__init__(session)
        self._ticker = ticker
        self._start_date = start_date

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def start_date(self) -> str:
        return self._start_date

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str, start_date: str = "1990-01-01") -> Self:
        cache_key = f"historical_prices/{ticker}_{start_date}.feather"
        cached_data = cls._cache.get(cache_key)

        data = cls(session, ticker, start_date)
        if cached_data is not None:
            data._data = cached_data
        else:
            await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_historical_prices(self._session, self._ticker, self._start_date)

        expiry = datetime.now() + relativedelta(months=6)
        cache_key = f"historical_prices/{self._ticker}_{self._start_date}.feather"
        self._cache.set(cache_key, self._data, expiry)

class IncomeStatementData(Data):
    def __init__(self, session: aiohttp.ClientSession, ticker: str, period: str):
        super().__init__(session)
        self._ticker = ticker
        self._period = period

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def period(self) -> str:
        return self._period

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str, period: str) -> Self:
        cached_data = cls._cache.get(f"income_statement_data_{period}/{ticker}.feather")

        data = cls(session, ticker, period)

        if cached_data is not None:
            data._data = cached_data
        else:
            await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_income_statement(self._session, self._ticker, self._period)

        expiry = datetime.now() + (relativedelta(months=3) if self._period == "quarter" else relativedelta(months=6))
        self._cache.set(f"income_statement_data_{self._period}/{self._ticker}.feather", self._data, expiry)

class BalanceSheetData(Data):
    def __init__(self, session: aiohttp.ClientSession, ticker: str, period: str):
        super().__init__(session)
        self._ticker = ticker
        self._period = period

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def period(self) -> str:
        return self._period

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str, period: str) -> Self:
        cached_data = cls._cache.get(f"balance_sheet_data_{period}/{ticker}.feather")

        data = cls(session, ticker, period)

        if cached_data is not None:
            data._data = cached_data
        else:
            await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_balance_sheet(self._session, self._ticker, self._period)

        expiry = datetime.now() + (relativedelta(months=3) if self._period == "quarter" else relativedelta(months=6))
        self._cache.set(f"balance_sheet_data_{self._period}/{self._ticker}.feather", self._data, expiry)

class CashFlowData(Data):
    def __init__(self, session: aiohttp.ClientSession, ticker: str, period: str = "quarter"):
        super().__init__(session)
        self._ticker = ticker
        self._period = period

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def period(self) -> str:
        return self._period

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str, period: str) -> Self:
        cached_data = cls._cache.get(f"cash_flow_data_{period}/{ticker}.feather")

        data = cls(session, ticker, period)

        if cached_data is not None:
            data._data = cached_data
        else:
            await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_cash_flow(self._session, self._ticker, self._period)

        expiry = datetime.now() + (relativedelta(months=3) if self._period == "quarter" else relativedelta(months=6))
        self._cache.set(f"cash_flow_data_{self._period}/{self._ticker}.feather", self._data, expiry)
