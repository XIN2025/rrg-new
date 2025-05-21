from datetime import datetime, timedelta
import time
import polars as pl
from modules.rrg.clickhouse_pool import ClickHousePool
from modules.rrg.logger import logger

def batch_load_eod_stock_data(dd_con, current_time, batch_years=2):
    """Load historical EOD stock data in batches."""
    try:
        # Calculate batch dates
        end_date = current_time
        start_date = end_date - timedelta(days=365 * batch_years)
        
        # Calculate number of batches (1 month per batch)
        num_batches = (end_date - start_date).days // 30 + 1
        
        # Process each batch
        for i in range(num_batches):
            batch_end = end_date - timedelta(days=30 * i)
            batch_start = batch_end - timedelta(days=30)
            
            # Stock data query
            batch_stock_query = batch_eod_stock_query_template.format(
                start_date=batch_start.strftime("%Y-%m-%d"),
                end_date=batch_end.strftime("%Y-%m-%d")
            )
            
            # Index data query
            batch_index_query = batch_eod_index_query_template.format(
                start_date=batch_start.strftime("%Y-%m-%d"),
                end_date=batch_end.strftime("%Y-%m-%d")
            )
            
            # Execute queries
            stock_result = ClickHousePool.execute_query(batch_stock_query)
            index_result = ClickHousePool.execute_query(batch_index_query)
            
            # Convert to DataFrames
            stock_df = pl.DataFrame(stock_result, schema=eod_stock_data_columns, orient="row")
            index_df = pl.DataFrame(index_result, schema=eod_stock_data_columns, orient="row")
            
            # Combine data
            batch_df = pl.concat([stock_df, index_df])
            
            if len(batch_df) > 0:
                # Log batch info
                logger.info(f"Batch {i+1}/{num_batches}: {len(batch_df)} rows")
                logger.info(f"Date range: {batch_start} to {batch_end}")
                logger.info(f"Unique symbols: {batch_df['symbol'].unique().to_list()}")
                
                # Convert to Arrow and insert
                df_arrow = batch_df.to_arrow()
                insert_result = dd_con.execute("INSERT INTO public.eod_stock_data SELECT * FROM df_arrow")
                logger.info(f"Inserted {insert_result.rowcount} rows")
            
            # Sleep to avoid overwhelming the database
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"Error in batch_load_eod_stock_data: {str(e)}", exc_info=True)
        raise

# Update query templates
batch_eod_stock_query_template = """
SELECT 
    ep.date_time AS created_at,
    ms.symbol,
    ep.close AS close_price,
    ep.security_code,
    lagInFrame(ep.close) OVER (PARTITION BY ep.security_code ORDER BY ep.date_time) AS previous_close,
    ms.ticker,
    ms.name,
    ms.slug,
    ms.security_type_code,
    ep.open,
    ep.high,
    ep.low,
    ep.volume,
    ep.turnover
FROM strike.equity_prices_1d AS ep
INNER JOIN strike.mv_stocks AS ms 
    ON ep.security_code = ms.security_code
WHERE ep.date_time > '{start_date}' AND ep.date_time <= '{end_date}'
    AND ep.close > 0  -- Ensure we only get valid prices
    AND ep.volume > 0  -- Ensure we only get valid volumes
ORDER BY ep.date_time DESC, ms.symbol
"""

batch_eod_index_query_template = """
SELECT 
    ip.date_time AS created_at,
    mi.symbol,
    ip.close AS close_price,
    ip.security_code,
    lagInFrame(ip.close) OVER (PARTITION BY ip.security_code ORDER BY ip.date_time) AS previous_close,
    mi.ticker,
    mi.name,
    mi.slug,
    mi.security_type_code,
    ip.open,
    ip.high,
    ip.low,
    ip.volume,
    ip.turnover
FROM strike.index_prices_1d AS ip
INNER JOIN strike.mv_indices AS mi 
    ON ip.security_code = mi.security_code
WHERE ip.date_time > '{start_date}' AND ip.date_time <= '{end_date}'
    AND ip.close > 0  -- Ensure we only get valid prices
    AND ip.volume > 0  -- Ensure we only get valid volumes
ORDER BY ip.date_time DESC, mi.symbol
""" 
