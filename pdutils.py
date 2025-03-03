import pandas as pd
import asyncio
import aiohttp

@pd.api.extensions.register_series_accessor("http")
class HttpAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if not isinstance(obj, pd.Series):
            raise AttributeError("a pd.Series is required")

    def get(self, concurrency=50):
        return asyncio.run(self.__get())

    async def __get(self):
        urls = self._obj.tolist()
        async def do_get(session, url):
            print(f"GET {url}")
            async with session.get(url) as resp:
                return [resp.status, await resp.text()]

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            tasks = [asyncio.create_task(do_get(session, url)) for url in urls]
            return await asyncio.gather(*tasks)
        
