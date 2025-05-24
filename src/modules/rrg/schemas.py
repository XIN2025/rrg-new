from typing import List, Optional, Dict, Any
from pydantic import BaseModel, validator
from datetime import datetime
import re

class RrgRequest(BaseModel):
    index_symbol: str
    timeframe: str
    date_range: str = "3650"  # Default to 10 years as string
    tickers: Optional[List[str]] = None
    channel_name: Optional[str] = None
    is_custom_index: bool = False
    skip_cache: bool = False

    @validator('date_range')
    def validate_date_range(cls, v):
        # If it's already an integer string, return it
        if v.isdigit():
            return v
            
        # Try to parse time period strings like "3 months", "6 weeks", etc.
        pattern = r'(\d+)\s*(day|week|month|year)s?'
        match = re.match(pattern, v.lower())
        if match:
            number, unit = match.groups()
            number = int(number)
            
            # Convert to days
            if unit == 'day':
                return str(number)
            elif unit == 'week':
                return str(number * 7)
            elif unit == 'month':
                return str(number * 30)
            elif unit == 'year':
                return str(number * 365)
                
        raise ValueError(f"Invalid date_range format: {v}. Expected format: '3 months', '6 weeks', '1 year', or number of days.")

class RrgResponse(BaseModel):
    status: str
    data: Dict[str, Any]
    filename: str
    error: Optional[str] = None
    cacheHit: bool = False

class StatusResponse(BaseModel):
    status: str 
