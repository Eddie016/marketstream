import io

import pyarrow as pa
import pyarrow.parquet as pq

from marketstream.processing.tables import MarketPrice


def price_to_parquet(price: MarketPrice) -> bytes:
    table = pa.table(
        {
            "event_id": [price.event_id],
            "schema_version": [price.schema_version],
            "provider": [price.provider],
            "symbol": [price.symbol],
            "trading_date": [price.trading_date],
            "open": [price.open],
            "high": [price.high],
            "low": [price.low],
            "close": [price.close],
            "volume": [price.volume],
            "currency": [price.currency],
            "snapshot_id": [price.snapshot_id],
        }
    )
    output = io.BytesIO()
    pq.write_table(table, output, compression="zstd", version="2.6")
    return output.getvalue()
