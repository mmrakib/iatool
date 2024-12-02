import asyncio

import pandas as pd
import aiohttp

from iatool.core.data import IncomeStatementData

async def main():
    async with aiohttp.ClientSession() as session:
        ticker = "AAPL"
        income_statement = await IncomeStatementData.create(session, ticker)
        print(income_statement.data)
        print()
        print(income_statement.data.loc["2023-09-30"])
        print()
        print(income_statement.data.loc["2023-09-30", "revenue"])
        print()
        print(income_statement.data["revenue"])

if __name__ == "__main__":
    asyncio.run(main())
