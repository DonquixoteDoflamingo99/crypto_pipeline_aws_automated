-- src/analytics/athena_queries.sql

-- 1. Price Performance Analysis
CREATE VIEW crypto_performance AS
SELECT 
    symbol,
    partition_date,
    AVG(price) as avg_daily_price,
    MAX(price) as max_daily_price,
    MIN(price) as min_daily_price,
    (MAX(price) - MIN(price)) / AVG(price) * 100 as daily_volatility,
    SUM(volume) as total_daily_volume,
    AVG(rsi) as avg_rsi,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count
FROM crypto_detailed
WHERE partition_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
GROUP BY symbol, partition_date
ORDER BY symbol, partition_date;

-- 2. Real-time Trading Signals
SELECT 
    symbol,
    price,
    rsi,
    macd_line,
    macd_signal,
    CASE 
        WHEN rsi < 30 AND macd_line > macd_signal THEN 'BUY_SIGNAL'
        WHEN rsi > 70 AND macd_line < macd_signal THEN 'SELL_SIGNAL'
        ELSE 'HOLD'
    END as trading_signal,
    processing_time
FROM crypto_detailed
WHERE partition_date = CURRENT_DATE
    AND rsi IS NOT NULL 
    AND macd_line IS NOT NULL
ORDER BY processing_time DESC;

-- 3. Market Correlation Analysis
WITH price_changes AS (
    SELECT 
        symbol,
        partition_date,
        AVG(price) as daily_price,
        LAG(AVG(price)) OVER (PARTITION BY symbol ORDER BY partition_date) as prev_price
    FROM crypto_detailed
    WHERE partition_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
    GROUP BY symbol, partition_date
),
daily_returns AS (
    SELECT 
        symbol,
        partition_date,
        (daily_price - prev_price) / prev_price * 100 as daily_return
    FROM price_changes
    WHERE prev_price IS NOT NULL
)
SELECT 
    a.symbol as symbol_a,
    b.symbol as symbol_b,
    CORR(a.daily_return, b.daily_return) as correlation_coefficient
FROM daily_returns a
JOIN daily_returns b ON a.partition_date = b.partition_date
WHERE a.symbol < b.symbol  -- Avoid duplicate pairs
GROUP BY a.symbol, b.symbol
HAVING COUNT(*) >= 30  -- At least 30 days of data
ORDER BY correlation_coefficient DESC;

-- 4. Anomaly Detection Summary
SELECT 
    symbol,
    partition_date,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count,
    COUNT(CASE WHEN is_anomaly THEN 1 END) * 100.0 / COUNT(*) as anomaly_percentage,
    AVG(CASE WHEN is_anomaly THEN ABS(z_score) END) as avg_anomaly_severity
FROM crypto_detailed
WHERE partition_date >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
GROUP BY symbol, partition_date
HAVING anomaly_count > 0
ORDER BY anomaly_percentage DESC;

-- 5. Volume Analysis
SELECT 
    symbol,
    DATE_TRUNC('hour', from_unixtime(timestamp)) as hour_bucket,
    SUM(volume) as hourly_volume,
    AVG(price) as avg_hourly_price,
    COUNT(*) as trade_frequency
FROM crypto_detailed
WHERE partition_date = CURRENT_DATE
GROUP BY symbol, DATE_TRUNC('hour', from_unixtime(timestamp))
ORDER BY symbol, hour_bucket;