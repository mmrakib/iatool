from typing import Self

import aiohttp

from .data import CompanyProfileData
from .data import HistoricalPricesData
from .data import IncomeStatementData
from .data import BalanceSheetData
from .data import CashFlowData

from .error import InputError
from .util import get_date_range

class Asset:
    def __init__(self, session: aiohttp.ClientSession, ticker: str):
        self._session = session
        self._ticker = ticker

        self._profile = None

        self._historical_prices = None

        self._income_statement_quarter = None
        self._balance_sheet_quarter = None
        self._cash_flow_quarter = None

        self._income_statement_annual = None
        self._balance_sheet_annual = None
        self._cash_flow_annual = None

    @property
    def session(self) -> aiohttp.ClientSession:
        return self._session

    @property
    def ticker(self) -> str:
        return self._ticker
    
    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        self = cls(session, ticker)
        
        self._profile = await CompanyProfileData.create(session, ticker)

        self._historical_prices = await HistoricalPricesData.create(session, ticker)

        self._income_statement_quarter = await IncomeStatementData.create(session, ticker, "quarter")
        self._balance_sheet_quarter = await BalanceSheetData.create(session, ticker, "quarter")
        self._cash_flow_quarter = await CashFlowData.create(session, ticker, "quarter")

        self._income_statement_annual = await IncomeStatementData.create(session, ticker, "annual")
        self._balance_sheet_annual = await BalanceSheetData.create(session, ticker, "annual")
        self._cash_flow_annual = await CashFlowData.create(session, ticker, "annual")

        return self
    
    async def update(self):
        self._profile.update()

        self._historical_prices.update()
        
        self._income_statement_quarter.update()
        self._balance_sheet_quarter.update()
        self._cash_flow_quarter.update()

        self._income_statement_annual.update()
        self._balance_sheet_annual.update()
        self._cash_flow_annual.update()

    def get_profile(self):
        return self._profile.data
    
    def get_historical_prices(self, start_date: str, end_date: str):
        filtered_data = get_date_range(self._historical_prices.data, start_date, end_date)

        return filtered_data

    def get_income_statement(self, start_date: str, end_date: str, period: str):
        if period == "quarter":
            filtered_data = get_date_range(self._income_statement_quarter.data, start_date, end_date)

            return filtered_data
        elif period == "annual":
            filtered_data = get_date_range(self._income_statement_annual.data, start_date, end_date)

            return filtered_data
        else:
            raise InputError("Invalid period")

    def get_balance_sheet(self, start_date: str, end_date: str, period: str):
        if period == "quarter":
            filtered_data = get_date_range(self._balance_sheet_quarter.data, start_date, end_date)

            return filtered_data
        elif period == "annual":
            filtered_data = get_date_range(self._balance_sheet_annual.data, start_date, end_date)

            return filtered_data
        else:
            raise InputError("Invalid period")
        
    def get_cash_flow(self, start_date: str, end_date: str, period: str):
        if period == "quarter":
            filtered_data = get_date_range(self._cash_flow_quarter.data, start_date, end_date)

            return filtered_data
        elif period == "annual":
            filtered_data = get_date_range(self._cash_flow_annual.data, start_date, end_date)

            return filtered_data
        else:
            raise InputError("Invalid period")
