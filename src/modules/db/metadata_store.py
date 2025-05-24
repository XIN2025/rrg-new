from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from datetime import datetime, timezone

logger = get_logger("metadata_store")

class RRGMetadataStore:
    """Class to handle metadata storage and retrieval for RRG module"""
    
    def __init__(self):
        self.last_refresh = None
        self.metadata = {}
        
    def get_market_metadata(self, tickers=None):
        """Get market metadata for given tickers or all if none specified"""
        try:
            with get_duckdb_connection() as conn:
                if not conn:
                    logger.error("Failed to establish DuckDB connection")
                    return None
                    
                query = """
                    SELECT 
                        name,
                        security_code,
                        ticker,
                        symbol,
                        security_type_code,
                        company_code,
                        meaningful_name
                    FROM public.market_metadata
                """
                
                if tickers:
                    query += " WHERE ticker IN ({})".format(
                        ','.join([f"'{t}'" for t in tickers])
                    )
                
                result = conn.execute(query).fetchall()
                
                if not result:
                    logger.warning("No market metadata found")
                    return None
                    
                # Convert to dictionary format
                metadata = {}
                for row in result:
                    metadata[row[2]] = {  # Use ticker as key
                        'name': row[0],
                        'security_code': row[1],
                        'ticker': row[2],
                        'symbol': row[3],
                        'security_type_code': row[4],
                        'company_code': row[5],
                        'meaningful_name': row[6]
                    }
                
                self.metadata = metadata
                self.last_refresh = datetime.now(timezone.utc)
                
                return metadata
                
        except Exception as e:
            logger.error(f"Error getting market metadata: {str(e)}")
            return None
            
    def get_last_refresh(self):
        """Get the timestamp of the last metadata refresh"""
        return self.last_refresh 
