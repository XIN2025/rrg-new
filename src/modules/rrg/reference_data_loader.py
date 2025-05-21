def load_market_metadata(dd_con=None, force=False):
    """Load market metadata from ClickHouse into DuckDB."""
    try:
        # Create necessary tables if they don't exist
        dd_con.execute("""
            CREATE TABLE IF NOT EXISTS public.market_metadata (
                security_code VARCHAR,
                symbol VARCHAR,
                ticker VARCHAR,
                name VARCHAR,
                slug VARCHAR,
                security_type_code VARCHAR,
                index_name VARCHAR,
                indices_slug VARCHAR,
                company_name VARCHAR,
                company_slug VARCHAR,
                sector VARCHAR,
                industry VARCHAR,
                market_cap DECIMAL,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                description VARCHAR,
                exchange VARCHAR,
                currency VARCHAR,
                country VARCHAR
            )
        """)

        dd_con.execute("""
            CREATE TABLE IF NOT EXISTS public.stock_prices (
                created_at TIMESTAMP,
                symbol VARCHAR,
                current_price DECIMAL,
                security_code VARCHAR,
                previous_close DECIMAL,
                ticker VARCHAR
            )
        """)

        dd_con.execute("""
            CREATE TABLE IF NOT EXISTS public.eod_stock_data (
                created_at TIMESTAMP,
                symbol VARCHAR,
                close_price DECIMAL,
                security_code VARCHAR,
                previous_close DECIMAL,
                ticker VARCHAR
            )
        """)

        # Log table creation
        logger.info("Created/verified tables in DuckDB:")
        tables = dd_con.execute("SHOW TABLES").fetchall()
        for table in tables:
            logger.info(f"Table: {table[0]}")
            # Show table schema
            schema = dd_con.execute(f"DESCRIBE {table[0]}").fetchall()
            logger.info(f"Schema for {table[0]}:")
            for col in schema:
                logger.info(f"  {col[0]}: {col[1]}")

        # Check if data already exists
        if not force:
            result = dd_con.execute("SELECT COUNT(*) FROM public.market_metadata").fetchone()
            if result[0] > 0:
                logger.info("Market metadata already exists, skipping load")
                return

        # Query for indices metadata
        metadata_query = """
        SELECT 
            mi.security_code,
            mi.symbol,
            mi.ticker,
            mi.name,
            mi.slug,
            mi.security_type_code,
            mi.index_name,
            mi.indices_slug,
            mi.company_name,
            mi.company_slug,
            mi.sector,
            mi.industry,
            mi.market_cap,
            mi.is_active,
            mi.created_at,
            mi.updated_at,
            mi.description,
            mi.exchange,
            mi.currency,
            mi.country
        FROM strike.mv_indices mi
        WHERE mi.is_active = 1
        AND mi.symbol IS NOT NULL
        AND mi.symbol != ''
        AND mi.security_type_code IN ('5', '26')
        """

        # Query for stocks metadata
        metadata_query2 = """
        SELECT 
            ms.security_code,
            ms.symbol,
            ms.ticker,
            ms.name,
            ms.slug,
            ms.security_type_code,
            ms.index_name,
            ms.indices_slug,
            ms.company_name,
            ms.company_slug,
            ms.sector,
            ms.industry,
            ms.market_cap,
            ms.is_active,
            ms.created_at,
            ms.updated_at,
            ms.description,
            ms.exchange,
            ms.currency,
            ms.country
        FROM strike.mv_stocks ms
        WHERE ms.is_active = 1
        AND ms.symbol IS NOT NULL
        AND ms.symbol != ''
        AND ms.security_type_code IN ('5', '26')
        """

        # Execute queries
        indices_result = ClickHousePool.execute_query(metadata_query)
        stocks_result = ClickHousePool.execute_query(metadata_query2)

        # Log sample data from ClickHouse
        logger.info("Sample data from ClickHouse mv_indices (first 3 rows):")
        for row in indices_result[:3]:
            logger.info(f"Indices row: {row}")

        logger.info("Sample data from ClickHouse mv_stocks (first 3 rows):")
        for row in stocks_result[:3]:
            logger.info(f"Stocks row: {row}")

        # Convert to DataFrames
        df1 = pl.DataFrame(indices_result, schema=market_metadata_columns, orient="row")
        df2 = pl.DataFrame(stocks_result, schema=market_metadata_columns, orient="row")

        # Log DataFrame info
        logger.info(f"Indices DataFrame shape: {df1.shape}")
        logger.info(f"Stocks DataFrame shape: {df2.shape}")
        
        # Log sample data from DataFrames
        logger.info("Indices DataFrame sample (first 3 rows):")
        logger.info(df1.head(3).to_dicts())
        logger.info("Stocks DataFrame sample (first 3 rows):")
        logger.info(df2.head(3).to_dicts())

        # Combine and deduplicate
        df = pl.concat([df1, df2]).unique(subset=["security_code"])

        # Log the combined data
        logger.info(f"Combined DataFrame shape: {df.shape}")
        logger.info("Combined DataFrame sample (first 3 rows):")
        logger.info(df.head(3).to_dicts())

        # Convert to Arrow and insert into DuckDB
        df_arrow = df.to_arrow()
        
        # Clear existing data if force=True
        if force:
            dd_con.execute("DELETE FROM public.market_metadata")
            logger.info("Cleared existing market_metadata data")
        
        # Insert new data
        insert_result = dd_con.execute("INSERT INTO public.market_metadata SELECT * FROM df_arrow")
        
        # Verify data after insert
        logger.info("Verifying data in DuckDB after insert:")
        result = dd_con.execute("""
            SELECT security_code, symbol, indices_slug, name 
            FROM public.market_metadata 
            LIMIT 3
        """).fetchall()
        logger.info(f"Sample data in DuckDB: {result}")

        logger.info(f"Inserted {insert_result.rowcount} rows into market_metadata")

    except Exception as e:
        logger.error(f"Error loading market metadata: {str(e)}", exc_info=True)
        raise 
