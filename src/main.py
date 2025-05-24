import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from src.modules.rrg.router import router as rrg_router
from src.modules.rrg.init import init_database
from src.utils.logger import get_logger
import argparse
from src.utils.duck_pool import get_duckdb_connection

# Configure logging
logger = get_logger(__name__)

def check_database_exists():
    """Check if database exists and has required tables"""
    try:
        with get_duckdb_connection() as conn:
            # Check if market_metadata table exists and has data
            result = conn.execute("""
                SELECT COUNT(*) as count 
                FROM public.market_metadata 
                WHERE security_type_code = 5
            """).fetchone()
            return result[0] > 0 if result else False
    except Exception as e:
        logger.error(f"Error checking database: {str(e)}")
        return False

def create_app(skip_data_load=False):
    # Create FastAPI app
    app = FastAPI(
        title="RRG API",
        description="RRG API for market data and analysis",
        version="1.0.0"
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(rrg_router)

    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {"status": "healthy"}

    @app.on_event("startup")
    async def startup_event():
        """Initialize the application on startup"""
        try:
            if skip_data_load:
                # Check if database exists and has data
                if not check_database_exists():
                    logger.warning("Database not found or empty. Loading data despite --skip-data-load flag.")
                    init_success = init_database()
                    if not init_success:
                        raise Exception("Database initialization failed")
                else:
                    logger.info("Skipping initial data load as requested - database exists")
            else:
                logger.info("Starting application initialization...")
                # Initialize database
                logger.info("Initializing database...")
                init_success = init_database()
                if not init_success:
                    raise Exception("Database initialization failed")
            
            logger.info("Application initialization completed successfully!")
            
        except Exception as e:
            logger.error(f"Application initialization failed: {str(e)}", exc_info=True)
            raise

    return app

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-data-load", action="store_true", help="Skip initial data load during startup")
    args = parser.parse_args()

    # Create app with appropriate skip_data_load setting
    app = create_app(skip_data_load=args.skip_data_load)
    
    # Run the server
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    # When running with uvicorn directly, create app without skipping data load
    app = create_app(skip_data_load=False) 
