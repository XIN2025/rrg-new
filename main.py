from contextlib import asynccontextmanager
import logging
import time
import os
from fastapi import FastAPI, Depends, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from config import API_CONFIG, CORS_CONFIG, LOGGING_CONFIG
from src.modules.rrg.router import router as rrg_router
from src.auth.jwt import get_current_user
from src.utils.manager.cache_manager import CacheManager
from src.modules.db.data_manager import load_data, clear_duckdb_locks
from src.utils.logger import configure_logging, get_logger
from src.modules.rrg.metadata_store import ensure_metadata_loaded, get_metadata_status, ensure_price_data_loaded, get_price_data_status
from src.utils.metrics import get_metrics, CONTENT_TYPE_LATEST

# Configure global logging
configure_logging(LOGGING_CONFIG.get("level"))
logger = get_logger("app")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup: initializing resources...")

    # Initialize Redis connections - non-critical components
    logger.info("Initializing Redis connections...")
    try:
        default_cache = CacheManager("default")
        if default_cache.redis_client:
            logger.info("Default Redis connection initialized successfully")
        else:
            logger.warning("Default Redis client initialized but connection may not be working")
    except Exception as e:
        logger.error(f"Failed to initialize default Redis connection: {str(e)}")
        logger.warning("Application will run with degraded functionality (no default caching)")

    try:
        rrg_cache = CacheManager("rrg")
        if rrg_cache.redis_client:
            logger.info("RRG Redis connection initialized successfully")
        else:
            logger.warning("RRG Redis client initialized but connection may not be working")
    except Exception as e:
        logger.error(f"Failed to initialize RRG Redis connection: {str(e)}")
        logger.warning("Application will run with degraded functionality (no RRG caching)")

    try:
        logger.info("Initializing DuckDB tables...")
        logger.info("Checking for any existing DuckDB locks...")
        clear_duckdb_locks()

        load_data_result = load_data()
        if not load_data_result:
            logger.warning("Failed to load reference and EOD data completely, some features may be limited")

        time.sleep(1)
        logger.info("DuckDB tables initialization process completed")

        logger.info("Loading metadata into global store for RRG module...")
        try:
            if ensure_metadata_loaded() and ensure_price_data_loaded():
                metadata_status = get_metadata_status()
                price_data_status = get_price_data_status()
                logger.info(f"RRG metadata loaded successfully: {metadata_status}")
                logger.info(f"RRG price data loaded successfully: {price_data_status}")
            else:
                logger.warning("Failed to load RRG metadata completely, RRG features may have degraded performance")
        except Exception as e:
            logger.error(f"Error loading RRG metadata: {str(e)}")
            logger.warning("RRG module will attempt to load metadata on first request")

    except Exception as e:
        logger.error(f"Failed to initialize DuckDB tables: {str(e)}")
        logger.warning("Application will run with degraded functionality (no RRG data)")

    yield

    try:
        logger.info("Closing Redis connections...")
        CacheManager.close_all()
        logger.info("Redis connections closed successfully")
    except Exception as e:
        logger.error(f"Error during application shutdown: {str(e)}")

def create_application() -> FastAPI:
    app = FastAPI(
        title=API_CONFIG["title"],
        description=API_CONFIG["description"],
        version=API_CONFIG["version"],
        docs_url=API_CONFIG["docs_url"],
        redoc_url=API_CONFIG["redoc_url"],
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_CONFIG["allow_origins"],
        allow_credentials=CORS_CONFIG["allow_credentials"],
        allow_methods=CORS_CONFIG["allow_methods"],
        allow_headers=CORS_CONFIG["allow_headers"],
    )

    app.include_router(rrg_router)

    @app.get("/health")
    async def health_check():
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics():
        try:
            return Response(content=get_metrics(), media_type=CONTENT_TYPE_LATEST)
        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}")
            return Response(content="", media_type=CONTENT_TYPE_LATEST)

    return app

app = create_application()

@app.get("/")
async def root(current_user: str = Depends(get_current_user)):
    return {"message": f"Welcome to Strike API, {current_user}"}
