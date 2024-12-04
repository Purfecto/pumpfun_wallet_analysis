WITH
  pump_wallets AS (
    SELECT DISTINCT trader_id AS wallet
    FROM dex_solana.trades
    WHERE project = 'pumpdotfun'
  ),
  
  datasales AS (
    SELECT
      block_time,
      tx_id,
      trader_id AS wallet,
      token_sold_mint_address AS token_address,
      COALESCE(token_sold_symbol, token_sold_mint_address) AS asset,
      -token_sold_amount AS amount,
      amount_usd,
      amount_usd AS usd_volume,
      0 AS token_price,
      amount_usd / NULLIF(token_sold_amount, 0) AS tp,
      'sell' AS action
    FROM
      dex_solana.trades
    WHERE 
      token_sold_mint_address NOT IN (
        'So11111111111111111111111111111111111111112',
        'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
      )
      AND token_sold_symbol NOT IN ('WETH', 'USDT', 'USDC')
      AND token_sold_mint_address ILIKE '%pump%'
      AND trader_id IN (SELECT wallet FROM pump_wallets)
    
    UNION ALL
    
    SELECT
      block_time,
      tx_id,
      trader_id AS wallet,
      token_bought_mint_address AS token_address,
      COALESCE(token_bought_symbol, token_bought_mint_address) AS asset,
      token_bought_amount AS amount,
      amount_usd,
      -amount_usd AS usd_volume,
      amount_usd / token_bought_amount AS token_price,
      amount_usd / NULLIF(token_bought_amount, 0) AS tp,
      'buy' AS action
    FROM
      dex_solana.trades
    WHERE
      token_bought_mint_address NOT IN (
        'So11111111111111111111111111111111111111112',
        'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
      )
      AND token_bought_symbol NOT IN ('WETH', 'USDT', 'USDC')
      AND token_bought_mint_address ILIKE '%pump%'
      AND trader_id IN (SELECT wallet FROM pump_wallets)
  ),
  
  lastprices AS (
    SELECT 
      token_address, 
      asset,
      tp AS token_price
    FROM (
      SELECT 
        token_address, 
        asset,
        tp,
        block_time,
        ROW_NUMBER() OVER (
          PARTITION BY token_address, asset 
          ORDER BY block_time DESC
        ) AS rn
      FROM 
        datasales
      WHERE 
        tp > 0
    ) AS ranked
    WHERE 
      rn = 1
  ),
  
  t AS (
    SELECT 
      wallet,
      token_address, 
      asset, 
      ROUND(SUM(CASE WHEN usd_volume < 0 THEN -usd_volume END), 2) AS buy,
      ROUND(SUM(CASE WHEN usd_volume > 0 THEN usd_volume END), 2) AS sell, 
      SUM(amount) AS balance, 
      SUM(usd_volume)  AS usdbalance
    FROM 
      datasales 
    WHERE 
      wallet IN (SELECT wallet FROM pump_wallets)
    GROUP BY 
      wallet, token_address, asset
  ),
  
  df1 AS (
    SELECT 
      DISTINCT a.wallet, a.asset, a.token_address, buy, sell, 
      CASE WHEN sell IS NOT NULL AND sell != 0 THEN ROUND(sell - buy, 2) END AS pnl,
      ROUND(balance * token_price, 2) AS usd_balance,
      CASE 
        WHEN sell IS NULL THEN ROUND(-buy + balance * token_price, 2)
        ELSE ROUND(sell - buy + balance * token_price, 2) 
      END AS total_pnl,
      ROUND(balance, 2) AS token_balance,
      token_price
    FROM 
      t a 
    JOIN 
      lastprices b ON a.asset = b.asset AND a.token_address = b.token_address
    WHERE 
      token_price < 2 AND buy IS NOT NULL
  )
  
SELECT
  wallet,
  total_pnl
FROM 
  df1
WHERE
  total_pnl > 1000000
ORDER BY
  total_pnl DESC;
