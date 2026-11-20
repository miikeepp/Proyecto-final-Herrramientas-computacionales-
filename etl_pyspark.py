"""ETL de ventas con PySpark.

Lee sales_data_sample.csv (sin modificarlo), lo limpia y lo guarda en
CSV (para Power BI), JSON y SQLite.
"""

import json
import sqlite3
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parent
SALIDA = RAIZ / "data" / "clean"
PORC_COSTO = 0.60  # el dataset no trae costo: se estima como 60 % del MSRP
CIUDADES = {"Gensve": "Genève", "Lule": "Luleå", "Aaarhus": "Aarhus", "Koln": "Köln",
            "Kobenhavn": "København", "Boras": "Borås", "NYC": "New York"}

spark = (SparkSession.builder.master("local[1]").appName("etl-ventas")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.legacy.javaCharsets", "true")  # permite leer IBM437
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1. EXTRACCIÓN: todo como texto, con la codificación real del archivo (IBM437)
crudo = spark.read.csv(str(RAIZ / "sales_data_sample.csv"), header=True, encoding="IBM437")
filas_entrada = crudo.count()

# Quitar espacios sobrantes y pasar textos vacíos a nulo
crudo = crudo.select([F.expr(f"NULLIF(REGEXP_REPLACE(TRIM({c}), ' +', ' '), '')").alias(c)
                      for c in crudo.columns])

# 2. TRANSFORMACIÓN: tipos y banderas de calidad en Spark SQL
crudo.createOrReplaceTempView("ventas_normalizadas")
df = spark.sql((RAIZ / "transformaciones_etl.sql").read_text(encoding="utf-8")).dropDuplicates()

# PRICEEACH viene truncado en 100: se recalcula desde SALES cuando no cuadra
precio_mal = F.abs(F.col("sales") - F.col("quantity_ordered") * F.col("price_each")) > 0.01
precios_corregidos = df.filter(precio_mal).count()
df = df.withColumn("price_each", F.when(precio_mal, F.round(F.col("sales") / F.col("quantity_ordered"), 2))
                                  .otherwise(F.col("price_each")))

banderas = [c for c in df.columns if c.startswith("invalid_")]
fecha = F.to_date("order_timestamp")
df = df.replace(CIUDADES, subset=["city"]).withColumns({
    "has_quality_error": F.expr(" OR ".join(banderas)),
    "es_venta_realizada": F.col("status") == "Shipped",
    "order_timestamp": fecha,
    "order_date": fecha,
    "year": F.year(fecha),
    "month": F.month(fecha),
    "quarter": F.quarter(fecha),
    "discount_pct": F.round((F.col("msrp") - F.col("price_each")) / F.col("msrp"), 4),
    "estimated_cost": F.round(F.col("msrp") * PORC_COSTO * F.col("quantity_ordered"), 2),
})
df = df.withColumn("profit", F.round(F.col("sales") - F.col("estimated_cost"), 2))
df = df.withColumn("margin_pct", F.round(F.col("profit") / F.col("sales"), 4))

# Decimales a double para que pandas, JSON y SQLite los manejen sin problema
df = df.select([F.col(c).cast("double") if t.startswith("decimal") else F.col(c) for c, t in df.dtypes])

# Spark ya hizo el trabajo; se pasa a pandas solo para escribir (evita winutils.exe en Windows)
ventas = df.orderBy("order_number", "order_line_number").toPandas()
spark.stop()

# 3. VALIDACIÓN
assert not ventas.duplicated(["order_number", "order_line_number"]).any(), "Hay líneas de pedido repetidas"
assert not ventas["has_quality_error"].any(), "Hay valores que no se pudieron convertir"
assert ((ventas["sales"] - ventas["quantity_ordered"] * ventas["price_each"]).abs() < 1).all(), "SALES no cuadra"

realizadas = ventas[ventas["es_venta_realizada"]]
resumen = {
    "filas_entrada": filas_entrada,
    "filas_salida": len(ventas),
    "duplicados_eliminados": filas_entrada - len(ventas),
    "precios_corregidos": precios_corregidos,
    "errores_de_calidad": int(ventas["has_quality_error"].sum()),
    "fecha_minima": str(ventas["order_date"].min()),
    "fecha_maxima": str(ventas["order_date"].max()),
    "pedidos_shipped": int(realizadas["order_number"].nunique()),
    "unidades_shipped": int(realizadas["quantity_ordered"].sum()),
    "ventas_shipped": round(float(realizadas["sales"].sum()), 2),
    "nulos_por_columna": {c: int(n) for c, n in ventas.isna().sum().items() if n > 0},
}
(RAIZ / "data" / "validation").mkdir(parents=True, exist_ok=True)
(RAIZ / "data" / "validation" / "validation_summary.json").write_text(
    json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")

# 4. CARGA EN CSV, JSON Y SQLITE
SALIDA.mkdir(parents=True, exist_ok=True)
ventas.to_csv(SALIDA / "sales_clean.csv", index=False, encoding="utf-8")
ventas.to_json(SALIDA / "sales_clean.json", orient="records", indent=2, force_ascii=False, date_format="iso")
with sqlite3.connect(SALIDA / "sales_clean.db") as con:
    ventas.astype({"order_timestamp": str, "order_date": str}).to_sql("ventas", con, if_exists="replace", index=False)

print(f"Filas: {filas_entrada} -> {len(ventas)} | Precios corregidos: {precios_corregidos}")
print(f"Ventas Shipped: {resumen['ventas_shipped']:,.2f} en {resumen['pedidos_shipped']} pedidos")