import asyncio

import aiohttp

from iatool.core.data import CompanyProfileData

async def main():
    async with aiohttp.ClientSession() as session:
        ticker = "AAPL"
        profile = await CompanyProfileData.create(session, ticker)
        print(profile.data)
        print()
        print(profile.data.loc["sector"])

if __name__ == "__main__":
    asyncio.run(main())
