import logging
from src.utils.metrics import TimerMetric

def split_time(days, timeframe):
    with TimerMetric("split_time", "rrg_generate"):
        day = int(timeframe.split(" ")[0])
        return day * days

def return_filter_days(timeframe):
    with TimerMetric("return_filter_days", "rrg_generate"):
        logging.debug(f"Calculating filter days for timeframe: {timeframe}")
        days = 0
        # Handle minute-based timeframes
        if timeframe.endswith('m'):
            minutes = int(timeframe[:-1])
            days = (minutes * 5) // 390 + 5
            logging.debug(f"Converted {minutes} minutes to {days} days")
            return days
        if timeframe == "daily":
            days = 252 + 30
        elif timeframe == "weekly":
            days = 252 + 60
        elif timeframe == "monthly":
            days = 252 + 90
        elif timeframe.endswith('h'):
            hours = int(timeframe[:-1])
            days = (hours * 5) // 6.5 + 5
        elif timeframe.endswith('d'):
            days = int(timeframe[:-1]) + 30
        elif "week" in timeframe:
            days = split_time(7, timeframe) + 60
        elif "month" in timeframe:
            days = split_time(30, timeframe) + 90
        elif "year" in timeframe:
            days = split_time(365, timeframe) + 200
        else:
            try:
                days = int(timeframe.split(" ")[0]) + 30
            except (ValueError, IndexError):
                logging.warning(f"Invalid timeframe format: {timeframe}, defaulting to 30 days")
                days = 30
        logging.debug(f"Calculated {days} filter days for {timeframe}")
        return days 
