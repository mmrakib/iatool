import asyncio

import pandas as pd
import aiohttp

from iatool.core.data import BalanceSheetData

async def main():
    async with aiohttp.ClientSession() as session:
        ticker = "AAPL"
        balance_sheet = await BalanceSheetData.create(session, ticker, "quarter")

        if balance_sheet is None:
            print("None balance sheet object")
            exit(1)

        print(balance_sheet.data)
        print()
        print(balance_sheet.data.loc["2023-09-30"])
        print()
        print(balance_sheet.data.loc["2023-09-30", "total_investments"])

if __name__ == "__main__":
    asyncio.run(main())
