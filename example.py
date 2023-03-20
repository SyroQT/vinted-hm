"""
Homework task for Vinted Data Engineering Academy
Example use of the implemented MapReduce framework
Author: Titas Janusonis
"""
import pandas as pd

from MapReduce import MapReduce

# task #1

MapReduce(
    maper={
        "data/clicks": lambda click: (click["date"], click["date"])
    },
    reducer=lambda kv: [{"date": kv[0], "count": kv[1].count(kv[0])}]
    ,
    output="data/clicks_per_day"
)

# task #2

def user_map(user: pd.Series) -> tuple[int, pd.Series]:
    if user["country"] == "LT":
        return  (user["id"], user.copy())

def country_reducer(kv: tuple[int, list]) -> list[dict]:
    index_list = kv[1][0].index.tolist()
    data_entries = kv[1]
    # only need entries with clicks and country info
    if index_list.count('country') > 0 and len(data_entries) > 1:
        df = pd.concat(data_entries[1:])
        df = df.groupby(level=0).agg(list).apply(pd.Series).T
        df[index_list] = data_entries[0]

        return df.to_dict(orient="records")


MapReduce(
    maper={
    "data/users": user_map,
    "data/clicks": lambda click: (click["user_id"], click.copy())
    },
    reducer=country_reducer, 
    output="data/filtered_clicks"
)
