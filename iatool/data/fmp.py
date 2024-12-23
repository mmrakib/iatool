import asyncio
from typing import List
from datetime import datetime

import aiohttp
import pandas as pd

from ..core.config import Config
from ..core.error import APIError
from ..core.error import InputError

endpoints = {
    "exchange_tickers": "/symbol/",
    "profile": "/profile/",
    "income_statement": "/income-statement/",
    "balance_sheet": "/balance-sheet-statement/",
    "cash_flow": "/cash-flow-statement/",
    "historical_prices": "/historical-price-full/",
}

async def fmp_fetch_data(
    session: aiohttp.ClientSession, 
    url: str, 
    args: List[str] = []
) -> List[str]:
    config = Config()
    base = config.api.fmp.base
    key = config.api.fmp.key

    args_str = "&".join(args)
    full_url = f"{base}{url}?apikey={key}&{args_str}"

    if args:
        print(f"Fetching data from: {base}{url}?{args_str}")
    else:
        print(f"Fetching data from: {base}{url}")

    while True:
        try:
            async with session.get(full_url) as response:
                if response.status == 429:  # Too many requests
                    await asyncio.sleep(config.api.retry_delay)
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientResponseError as http_error:
            raise APIError(f"{http_error}")
        except aiohttp.ClientConnectionError as conn_error:
            raise APIError(f"{conn_error}")
        except aiohttp.ClientPayloadError as payload_error:
            raise APIError(f"{payload_error}")
        except Exception as error:
            raise APIError(f"{error}")
        
async def fmp_fetch_all_tickers_exchange(
    session: aiohttp.ClientSession,
    exchange: str
) -> pd.Series:
    raw_data = await fmp_fetch_data(session, f"{endpoints["exchange_tickers"]}{exchange}")

    if not raw_data:
        raise APIError("No data found")

    tickers = [item["symbol"] for item in raw_data]

    df = pd.Series(tickers)

    return df

async def fmp_fetch_company_profile(
    session: aiohttp.ClientSession,
    ticker: str
) -> pd.Series:
    raw_data = await fmp_fetch_data(session, f"{endpoints["profile"]}{ticker}")
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

    if not raw_data:
        raise APIError("No data found")

    mapped_data = {key_mapping.get(k, k): v for k, v in raw_data.items()}
    ds = pd.Series(mapped_data)
    
    return ds

async def fmp_fetch_historical_prices(
    session: aiohttp.ClientSession,
    ticker: str,
    start_date: str = "1990-01-01",
) -> pd.DataFrame:
    raw_data = await fmp_fetch_data(session, f"{endpoints['historical_prices']}{ticker}", [f"from={start_date}"])

    if not raw_data or "historical" not in raw_data:
        return pd.DataFrame()

    dates = []
    open_prices = []
    high_prices = []
    low_prices = []
    close_prices = []
    adj_close_prices = []
    volumes = []
    unadjusted_volumes = []
    changes = []
    change_percent = []
    vwap = []
    labels = []
    change_over_time = []

    for entry in raw_data["historical"]:
        dates.append(datetime.strptime(entry["date"], "%Y-%m-%d"))
        open_prices.append(entry["open"])
        high_prices.append(entry["high"])
        low_prices.append(entry["low"])
        close_prices.append(entry["close"])
        adj_close_prices.append(entry["adjClose"])
        volumes.append(entry["volume"])
        unadjusted_volumes.append(entry["unadjustedVolume"])
        changes.append(entry["change"])
        change_percent.append(entry["changePercent"])
        vwap.append(entry["vwap"])
        labels.append(entry["label"])
        change_over_time.append(entry["changeOverTime"])

    df = pd.DataFrame({
        "date": pd.to_datetime(dates),
        "open": open_prices,
        "high": high_prices,
        "low": low_prices,
        "close": close_prices,
        "adj_close": adj_close_prices,
        "volume": volumes,
        "unadjusted_volume": unadjusted_volumes,
        "change": changes,
        "change_percent": change_percent,
        "vwap": vwap,
        "label": labels,
        "change_over_time": change_over_time
    })

    df.set_index("date", inplace=True)

    return df

async def fmp_fetch_income_statement(
    session: aiohttp.ClientSession, 
    ticker: str, 
    period: str
) -> pd.DataFrame:
    if period not in ["quarter", "annual"]:
        raise InputError("Period must be either 'quarter' or 'annual'")

    raw_data = await fmp_fetch_data(session, f"{endpoints["income_statement"]}{ticker}", [f"period={period}"])

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

    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    df.rename(columns=key_mapping, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df

async def fmp_fetch_balance_sheet(
    session: aiohttp.ClientSession, 
    ticker: str,
    period: str
) -> pd.DataFrame:
    if period not in ["quarter", "annual"]:
        raise InputError("Period must be either 'quarter' or 'annual'")

    raw_data = await fmp_fetch_data(session, f"{endpoints["balance_sheet"]}{ticker}", [f"period={period}"])

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

    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    df.rename(columns=key_mapping, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df

async def fmp_fetch_cash_flow(
    session: aiohttp.ClientSession, 
    ticker: str,
    period: str
) -> pd.DataFrame:
    if period not in ["quarter", "annual"]:
        raise InputError("Period must be either 'quarter' or 'annual'")

    raw_data = await fmp_fetch_data(session, f"{endpoints["cash_flow"]}{ticker}", [f"period={period}"])

    key_mapping = {
        "date": "date",
        "symbol": "ticker",
        "reportedCurrency": "currency",
        "cik": "cik",
        "fillingDate": "filing_date",
        "acceptedDate": "accepted_date",
        "calendarYear": "year",
        "period": "period",
        "netIncome": "net_income",
        "depreciationAndAmortization": "depreciation_and_amortization",
        "deferredIncomeTax": "deferred_income_tax",
        "stockBasedCompensation": "stock_based_compensation",
        "changeInWorkingCapital": "change_in_working_capital",
        "accountsReceivables": "accounts_receivable",
        "inventory": "inventory",
        "accountsPayables": "accounts_payable",
        "otherWorkingCapital": "other_working_capital",
        "otherNonCashItems": "other_noncash_items",
        "netCashProvidedByOperatingActivities": "net_cash_provided_by_operating_activities",
        "investmentsInPropertyPlantAndEquipment": "investments_in_property_plant_and_equipment",
        "acquisitionsNet": "net_acquisitions",
        "purchasesOfInvestments": "purchases_of_investments",
        "salesMaturitiesOfInvestments": "sales_and_maturities_of_investments",
        "otherInvestingActivites": "other_investing_activities",
        "netCashUsedForInvestingActivites": "net_cash_used_for_investing_activities",
        "debtRepayment": "debt_repayment",
        "commonStockIssued": "common_stock_issued",
        "commonStockRepurchased": "common_stock_repurchased",
        "dividendsPaid": "dividends_paid",
        "otherFinancingActivites": "other_financing_activities",
        "netCashUsedProvidedByFinancingActivities": "net_cash_used_provided_by_financing_activities",
        "effectOfForexChangesOnCash": "effect_of_forex_changes_on_cash",
        "netChangeInCash": "net_change_in_cash",
        "cashAtEndOfPeriod": "cash_at_end",
        "cashAtBeginningOfPeriod": "cash_at_beginning",
        "operatingCashFlow": "operating_cash_flow",
        "capitalExpenditure": "capital_expenditure",
        "freeCashFlow": "free_cash_flow",
        "link": "filing_link",
        "finalLink": "final_filing_link",
    }

    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    df.rename(columns=key_mapping, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df
