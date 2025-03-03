import pandas as pd
import pdutils
import pytest


# @pytest.mark.asyncio
def test_get():
    urls = pd.Series([
        # f"http://localhost:8080/200",
        # f"http://localhost:8080/400"
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/400",
    ] * 20) 

    resps = urls.http.get()
    assert len(resps) == len(urls)
    assert resps[0][0] == 200
    assert resps[1][0] == 400
