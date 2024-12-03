from abc import abstractmethod
from typing import Self, Union

import aiohttp
import pandas as pd

from ..data.fmp import fmp_fetch_company_profile
from ..data.fmp import fmp_fetch_income_statement
from ..data.fmp import fmp_fetch_balance_sheet
from ..data.fmp import fmp_fetch_cash_flow
from ..data.fmp import fmp_fetch_all_tickers_exchange

class Data:
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
        data = cls(session, exchange)
        await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_all_tickers_exchange(self._session, self._exchange)

class CompanyProfileData(Data):
    def __init__(self, session: aiohttp.ClientSession, ticker: str):
        super().__init__(session)
        self._ticker = ticker

    @property
    def ticker(self) -> str:
        return self._ticker

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        data = cls(session, ticker)
        await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_company_profile(self._session, self._ticker)

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
        data = cls(session, ticker, period)
        await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_income_statement(self._session, self._ticker, self._period)

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
        data = cls(session, ticker, period)
        await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_balance_sheet(self._session, self._ticker, self._period)

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
        data = cls(session, ticker, period)
        await data.update()

        return data
    
    async def update(self):
        self._data = await fmp_fetch_cash_flow(self._session, self._ticker, self._period)
