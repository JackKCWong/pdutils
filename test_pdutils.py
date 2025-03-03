import pandas as pd
import pdutils
import pytest


def test_http_get():
    urls = pd.Series([
        # f"http://localhost:8080/200",
        # f"http://localhost:8080/400"
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/400",
    ] * 20) 

    resps = urls.http.get(20)
    assert len(resps) == len(urls)
    assert resps[0][0] == 200
    assert resps[1][0] == 400


def test_df_grep():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': ["a", "b", "C"]
    })

    assert df.txt.grep("1")["A"].iloc[0] == 1
    assert df.txt.grep("b")["A"].iloc[0] == 2
    assert df.txt.grep("c")["A"].iloc[0] == 3

    assert len(df.txt.egrep("a|b")) == 2