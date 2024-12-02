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
            "fillingDate": "filing_date",
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

class BalanceSheetData(Data):
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
        raw_data = await fetch_data_fmp(session, f"/balance-sheet-statement/{self._ticker}", [f"period={self._period}"])

        key_mapping = {
            "date": "date",
            "symbol": "ticker",
            "reportedCurrency": "currency",
            "cik": "cik",
            "fillingDate": "filing_date",
            "acceptedDate": "accepted_date",
            "calendarYear": "year",
            "period": "period",
            "cashAndCashEquivalents": "cash_and_cash_equivalents",
            "shortTermInvestments": "short_term_investments",
            "cashAndShortTermInvestments": "cash_and_short_term_investments",
            "netReceivables": "net_receivables",
            "inventory": "inventory",
            "otherCurrentAssets": "other_current_assets",
            "totalCurrentAssets": "total_current_assets",
            "propertyPlantEquipmentNet": "net_property_plant_and_equipment",
            "goodwill": "goodwill",
            "intangibleAssets": "intangible_assets",
            "goodwillAndIntangibleAssets": "goodwill_and_intangible_assets",
            "longTermInvestments": "long_term_investments",
            "taxAssets": "tax_assets",
            "otherNonCurrentAssets": "other_noncurrent_assets",
            "totalNonCurrentAssets": "total_noncurrent_assets",
            "otherAssets": "other_assets",
            "totalAssets": "total_assets",
            "accountPayables": "accounts_payable",
            "shortTermDebt": "short_term_debt",
            "taxPayables": "tax_payables",
            "deferredRevenue": "deferred_revenue",
            "otherCurrentLiabilities": "other_current_liabilities",
            "totalCurrentLiabilities": "total_current_liabilities",
            "longTermDebt": "long_term_debt",
            "deferredRevenueNonCurrent": "deferred_noncurrent_revenue",
            "deferredTaxLiabilitiesNonCurrent": "deferred_noncurrent_tax_liabilities",
            "otherNonCurrentLiabilities": "other_noncurrent_liabilities",
            "totalNonCurrentLiabilities": "total_noncurrent_liabilities",
            "otherLiabilities": "other_liabilities",
            "capitalLeaseObligations": "capital_lease_obligations",
            "totalLiabilities": "total_liabilities",
            "preferredStock": "preferred_stock",
            "commonStock": "common_stock",
            "retainedEarnings": "retained_earnings",
            "accumulatedOtherComprehensiveIncomeLoss": "accumulated_other_comprehensive_income_loss",
            "othertotalStockholdersEquity": "other_total_stockholders_equity",
            "totalStockholdersEquity": "total_stockholders_equity",
            "totalEquity": "total_equity",
            "totalLiabilitiesAndStockholdersEquity": "total_liabilities_and_stockholders_equity",
            "minorityInterest": "minority_interest",
            "totalLiabilitiesAndTotalEquity": "total_liabilities_and_total_equity",
            "totalInvestments": "total_investments",
            "totalDebt": "total_debt",
            "netDebt": "net_debt",
            "link": "filing_link",
            "finalLink": "final_filing_link",
        }

        df = pd.DataFrame(raw_data)
        df.rename(columns=key_mapping, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        self._data = df