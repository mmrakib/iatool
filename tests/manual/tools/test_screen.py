import asyncio

import aiohttp

from iatool.core.asset import Asset
from iatool.core.search import SearchTool
from iatool.core.screen import ScreenTool

def pred(asset: Asset) -> bool:
    income_statement = asset.get_income_statement("2020-01-01", "2021-01-01", "quarter")
    revenue_threshold = 100000
    
    if income_statement.empty:
        print(f"Found one with no income statement: {asset.ticker}")
        return

    if not "revenue" in income_statement.columns:
        print(f"Found one with no revenue: {asset.ticker}")
        return

    if income_statement["revenue"].mean() > revenue_threshold:
        print(f"Found one with revenue more than {revenue_threshold}")
        return True
    else:
        print(f"Found one with revenue less than or equal to {revenue_threshold}")
        return False

async def main():
    async with aiohttp.ClientSession() as session:
        search = SearchTool(session)

        asx_tickers = await search.get_all_tickers_exchange("ASX")
        asx_tickers = asx_tickers[100:105]
        
        assets = []

        for ticker in asx_tickers:
            asset = await Asset.create(session, ticker)
            
            assets.append(asset)

        screen = ScreenTool()
        results = screen.run(assets, pred)

        for asset in results:
            print(asset.ticker)

if __name__ == "__main__":
    asyncio.run(main())
