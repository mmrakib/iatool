import asyncio

import pandas as pd
import aiohttp

from iatool.core.data import CashFlowData

async def main():
    async with aiohttp.ClientSession() as session:
        ticker = "AAPL"
        cash_flow = await CashFlowData.create(session, ticker)
        print(cash_flow.data)
        print()
        print(cash_flow.data.loc["2023-09-30"])
        print()
        print(cash_flow.data.loc["2023-09-30", "net_income"])

if __name__ == "__main__":
    asyncio.run(main())
