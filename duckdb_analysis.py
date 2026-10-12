import duckdb

con = duckdb.connect("analytics.duckdb")

result = con.execute("""
    SELECT COUNT(*) AS total_orders
    FROM orders
""").fetchone()

print(f"Nombre de commandes : {result[0]}")

con.close()