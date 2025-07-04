import asyncio, json, pandas as pd
from collections import deque
from nats.aio.client import Client as NATS

BATCH_SIZE = 10_000
SUBJECT     = "marketorders.deduped"
URL         = "nats://public:thenewalbiondata@nats.albion-online-data.com:4222"

buffer = deque()

async def handler(msg):
    buffer.append(json.loads(msg.data))
    if len(buffer) >= BATCH_SIZE:
        df = pd.DataFrame(list(buffer))
        buffer.clear()
        process(df)

def process(df: pd.DataFrame):
    df["unit_price"] = df["price"] / df["amount"]
    resumo = (df.query("quality >= 3")
                .groupby(["item_id"], as_index=False)
                .agg(avg_price=("unit_price", "mean"),
                     volume=("amount", "sum")))
    resumo.to_parquet("data/market_summary.parquet",
                      partition_cols=["item_id"],
                      index=False,
                      compression="zstd")

async def main():
    nc = NATS()
    await nc.connect(servers=[URL],
                     name="pandas-consumer",
                     max_reconnect_attempts=-1)
    await nc.subscribe(SUBJECT, queue="workers", cb=handler)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())