import asyncio

import aiohttp

from iatool.core.search import SearchTool
from iatool.core.screen import ScreenTool

async def main():
    async with aiohttp.ClientSession() as session:
        search = SearchTool(session)

        asx_tickers = await search.get_all_tickers_exchange("ASX")

        for ticker in asx_tickers:
            print(ticker)

if __name__ == "__main__":
    asyncio.run(main())
