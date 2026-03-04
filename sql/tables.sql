-- Create schema
CREATE SCHEMA trades;

-- Raw events table
CREATE TABLE trades.raw_events (
    trade_id            BIGINT,
    product_id          VARCHAR(20),
    price               DECIMAL(18,8),
    size                DECIMAL(18,8),
    side                VARCHAR(10),
    trade_timestamp     TIMESTAMP,
    trade_value_usd     DECIMAL(18,2),
    size_category       VARCHAR(20),
    ingested_at         TIMESTAMP,
    source_system       VARCHAR(50) DEFAULT 'COINBASE_WEBSOCKET'
);

-- Trade summary table
CREATE TABLE trades.trade_summary (
    summary_id          INT IDENTITY(1,1) PRIMARY KEY,
    product_id          VARCHAR(20),
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    total_trades        INT,
    total_volume        DECIMAL(18,8),
    avg_price           DECIMAL(18,8),
    buy_count           INT,
    sell_count          INT,
    high_price          DECIMAL(18,8),
    low_price           DECIMAL(18,8)
);

-- 1-minute aggregation stored procedure
CREATE OR REPLACE PROCEDURE trades.sp_aggregate_trades()
AS $$
BEGIN
    INSERT INTO trades.trade_summary (
        product_id, window_start, window_end,
        total_trades, total_volume, avg_price,
        buy_count, sell_count, high_price, low_price
    )
    SELECT
        product_id,
        DATEADD(minute, -1, GETDATE())  AS window_start,
        GETDATE()                        AS window_end,
        COUNT(*)                         AS total_trades,
        SUM(size)                        AS total_volume,
        AVG(price)                       AS avg_price,
        SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END)  AS buy_count,
        SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) AS sell_count,
        MAX(price)                       AS high_price,
        MIN(price)                       AS low_price
    FROM trades.raw_events
    WHERE trade_timestamp >= DATEADD(minute, -1, GETDATE())
    GROUP BY product_id;
END;
$$ LANGUAGE plpgsql;