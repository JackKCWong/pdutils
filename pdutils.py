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
        resps = asyncio.run(self.__get(concurrency))
        return pd.Series(resps, index=self._obj.index)

    async def __get(self, concurrency):
        urls = self._obj.tolist()
        sem = asyncio.Semaphore(concurrency)
        async def do_get(session, url):
            try:
                async with sem:
                    async with session.get(url) as resp:
                        return [resp.status, await resp.text()]
            except Exception as e:
                return [0, str(e)]

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            tasks = [asyncio.create_task(do_get(session, url)) for url in urls]
            return await asyncio.gather(*tasks)
        

@pd.api.extensions.register_dataframe_accessor("txt")
class TextAccessor:
    def __init__(self, pandas_obj):
        if not isinstance(pandas_obj, pd.DataFrame):
            raise AttributeError("a pd.DataFrame is required")
        self._df = pandas_obj

    def grep(self, text):
        return self._df[self._df.apply(lambda row: row.astype(str).str.contains(text, case=False, regex=False).any(), axis=1)]

    def egrep(self, pat):
        return self._df[self._df.apply(lambda row: row.astype(str).str.contains(pat).any(), axis=1)]
