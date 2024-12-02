import asyncio
from typing import List

import aiohttp

from .config import Config
from .error import APIError

async def fetch_data_fmp(session: aiohttp.ClientSession, url: str, args: List[str] = []) -> List[str]:
    config = Config()

    args_str = "&".join(args)
    full_url = f"{config.api.fmp.base_url}{url}?apikey={config.api.fmp.key}&{args_str}"

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
