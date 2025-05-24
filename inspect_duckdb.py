import duckdb

DB_PATH = 'data/pydb.duckdb'

con = duckdb.connect(DB_PATH)

print('--- TABLES ---')
tables = con.execute("SHOW TABLES").fetchall()
for (table,) in tables:
    print(f'\nTable: {table}')
    # Print schema
    schema = con.execute(f"DESCRIBE {table}").fetchall()
    print('Schema:')
    for col in schema:
        print(f'  {col[0]}: {col[1]}')
    # Print sample data
    sample = con.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
    print('Sample data:')
    print(sample)

con.close() 
