from abc import abstractmethod
from typing import Self, Union

import aiohttp
import pandas as pd

from ..data.fmp import fmp_fetch_company_profile
from ..data.fmp import fmp_fetch_income_statement
from ..data.fmp import fmp_fetch_balance_sheet
from ..data.fmp import fmp_fetch_cash_flow

class Data:
    def __init__(self):
        self._data = None
    
    @property
    def data(self) -> Union[pd.Series, pd.DataFrame]:
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

    @property
    def ticker(self) -> str:
        return self._ticker

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        data = cls(ticker)
        await data.update(session)

        return data
    
    async def update(self, session: aiohttp.ClientSession):
        self._data = await fmp_fetch_company_profile(session, self._ticker)

class IncomeStatementData(Data):
    def __init__(self, ticker: str, period: str):
        super().__init__()
        self._ticker = ticker
        self._period = period

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def period(self) -> str:
        return self._period

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        data = cls(ticker)
        await data.update(session)

        return data
    
    async def update(self, session: aiohttp.ClientSession):
        self._data = await fmp_fetch_income_statement(session, self._ticker, self._period)

class BalanceSheetData(Data):
    def __init__(self, ticker: str, period: str):
        super().__init__()
        self._ticker = ticker
        self._period = period

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def period(self) -> str:
        return self._period

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        data = cls(ticker)
        await data.update(session)

        return data
    
    async def update(self, session: aiohttp.ClientSession):
        self._data = await fmp_fetch_balance_sheet(session, self._ticker, self._period)

class CashFlowData(Data):
    def __init__(self, ticker: str, period: str = "quarter"):
        super().__init__()
        self._ticker = ticker
        self._period = period

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def period(self) -> str:
        return self._period

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        data = cls(ticker)
        await data.update(session)

        return data
    
    async def update(self, session: aiohttp.ClientSession):
        self._data = await fmp_fetch_cash_flow(session, self._ticker, self._period)
