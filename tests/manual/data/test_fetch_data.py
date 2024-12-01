import asyncio

import aiohttp

from iatool.core.data import fetch_data

async def main():
    async with aiohttp.ClientSession() as session:
        url = "/profile/AAPL"
        data = await fetch_data(session, url)

        print(data)

if __name__ == "__main__":
    asyncio.run(main())
