
-- 
-- Average price per coin per operation
-- 
CREATE VIEW `crypto-prices-22175.cryptos.crypto_price_analytics_minute` AS
SELECT
  symbol, operation, 
  TIMESTAMP_TRUNC(event_time, MINUTE) AS minute,
  AVG(price) AS avg_price
FROM `crypto-prices-22175.cryptos.prices` 
GROUP BY symbol, operation, minute