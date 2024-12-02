from abc import abstractmethod
from typing import Self

import pandas as pd
import aiohttp

from ..data.fmp import fetch_data_fmp
from .error import InputError

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

class IncomeStatementData(Data):
    def __init__(self, ticker: str, period: str = "quarter"):
        if period not in ["quarter", "annual"]:
            raise InputError("Period must be either 'quarter' or 'annual'")

        super().__init__()
        self._ticker = ticker
        self._period = period

    @property
    def period(self) -> str:
        return self._period

    @classmethod
    async def create(cls, session: aiohttp.ClientSession, ticker: str) -> Self:
        data = cls(ticker)
        await data.update(session)

        return data
    
    async def update(self, session: aiohttp.ClientSession):
        raw_data = await fetch_data_fmp(session, f"/income-statement/{self._ticker}", [f"period={self._period}"])

        key_mapping = {
            "date": "date",
            "symbol": "ticker",
            "reportedCurrency": "currency",
            "cik": "cik",
            "filingDate": "filing_date",
            "acceptedDate": "accepted_date",
            "calendarYear": "year",
            "period": "period",
            "revenue": "revenue",
            "costOfRevenue": "cost_of_revenue",
            "grossProfit": "gross_profit",
            "grossProfitRatio": "gross_profit_ratio",
            "researchAndDevelopmentExpenses": "research_and_development_expenses",
            "generalAndAdministrativeExpenses": "general_and_administrative_expenses",
            "sellingAndMarketingExpenses": "selling_and_marketing_expenses",
            "sellingGeneralAndAdministrativeExpenses": "selling_general_and_administrative_expenses",
            "otherExpenses": "other_expenses",
            "operatingExpenses": "operating_expenses",
            "costAndExpenses": "cost_and_expenses",
            "interestExpense": "interest_expenses",
            "depreciationAndAmortization": "depreciation_and_amortization",
            "ebitda": "ebitda",
            "ebitdaratio": "ebitda_ratio",
            "operatingIncome": "operating_income",
            "operatingIncomeRatio": "operating_income_ratio",
            "totalOtherIncomeExpensesNet": "net_total_other_income_expenses",
            "incomeBeforeTax": "income_before_tax",
            "incomeBeforeTaxRatio": "income_before_tax_ratio",
            "incomeTaxExpense": "income_tax_expenses",
            "netIncome": "net_income",
            "netIncomeRatio": "net_income_ratio",
            "eps": "eps",
            "epsdiluted": "eps_diluted",
            "weightedAverageShsOut": "weighted_average_shares_outstanding",
            "weightedAverageShsOutDil": "weighted_average_shares_outstanding_diluted",
            "link": "filing_link",
            "finalLink": "final_filing_link",
        }

        df = pd.DataFrame(raw_data)
        df.rename(columns=key_mapping, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        self._data = df
