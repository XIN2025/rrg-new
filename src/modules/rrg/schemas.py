from typing import List
from pydantic import BaseModel

class RrgRequest(BaseModel):
    index_symbol: str
    date_range: str | None = None
    timeframe: str
    channel_name: str | None = None
    is_indices: bool | None = None
    tickers: List[str]
    last_traded_time: str
    is_custom_index: bool
    skip_cache: bool = False

class RrgResponse(BaseModel):
    data: dict
    change_data: List[dict] | None
    filename: str
    cacheHit: bool

class StatusResponse(BaseModel):
    status: str 