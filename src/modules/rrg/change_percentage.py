import os
import polars as pl
from log import logger

def eod_change_percentage_file(data, filename, cache_manager, timeframe, date_range):
    """
    Calculate end-of-day change percentages for a given timeframe.
    
    Args:
        data: DataFrame containing price data
        filename: Name of the output file
        cache_manager: Cache manager instance
        timeframe: Timeframe for calculation (e.g., '1d', '1w', '1m', '3m', '6m', '1y')
        date_range: Date range for calculation
        
    Returns:
        DataFrame with change percentages
    """
    logger.info(f"[RRG][CHANGE PERCENTAGE] Starting calculation for timeframe: {timeframe}")
    
    try:
        # Validate input data
        if data.is_empty():
            logger.error("[RRG][CHANGE PERCENTAGE] Input data is empty")
            return None
            
        # Get benchmark symbol (first column after column1)
        benchmark = data.columns[1]
        logger.debug(f"[RRG][CHANGE PERCENTAGE] Using benchmark: {benchmark}")
        
        # Convert timeframe to days
        timeframe_days = {
            '1d': 1,
            '1w': 5,
            '1m': 21,
            '3m': 63,
            '6m': 126,
            '1y': 252
        }.get(timeframe, 1)
        
        # Calculate change percentages
        result_data = []
        for col in data.columns:
            if col == 'column1':  # Skip date column
                continue
                
            # Get price series
            prices = data[col].cast(pl.Float64)
            
            # Calculate rolling changes
            if timeframe == '1d':
                # For 1-day changes, use simple percentage change
                changes = prices.pct_change() * 100
            else:
                # For other timeframes, use rolling window
                changes = prices.pct_change(periods=timeframe_days) * 100
            
            # Add to result
            result_data.append({
                'column1': col,
                benchmark: changes
            })
        
        # Create result DataFrame
        result_df = pl.DataFrame(result_data)
        
        # Sort by benchmark performance
        result_df = result_df.sort(by=benchmark, descending=True)
        
        # Write to file
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        result_df.write_csv(filename, separator="\t", has_header=False)
        
        logger.info(f"[RRG][CHANGE PERCENTAGE] Successfully wrote results to: {filename}")
        return result_df
        
    except Exception as e:
        logger.error(f"[RRG][CHANGE PERCENTAGE] Error calculating change percentages: {str(e)}", exc_info=True)
        return None 
