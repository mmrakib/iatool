import asyncio

import aiohttp

from iatool.core.data import fetch_data_fmp

async def main():
    async with aiohttp.ClientSession() as session:
        url = "/profile/AAPL"
        data = await fetch_data_fmp(session, url)

        print(data)

if __name__ == "__main__":
    asyncio.run(main())
