"""ETL de ventas con PySpark.

Extrae sales_data_sample.csv (sin modificarlo), lo limpia y lo carga en
tres formatos: CSV (para Power BI), JSON y SQLite.
"""

import json
import sqlite3
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parent
ORIGEN = RAIZ / "sales_data_sample.csv"
SQL_TIPOS = RAIZ / "transformaciones_etl.sql"
CARPETA_LIMPIA = RAIZ / "data" / "clean"
RESUMEN = RAIZ / "data" / "validation" / "validation_summary.json"

PORC_COSTO = 0.60  # supuesto: el dataset no trae costo, se estima como 60 % del MSRP

# Nombres de ciudad mal escritos en el archivo original
CIUDADES = {
    "Gensve": "Genève", "Lule": "Luleå", "Aaarhus": "Aarhus", "Koln": "Köln",
    "Kobenhavn": "København", "Boras": "Borås", "NYC": "New York",
}

spark = (
    SparkSession.builder.master("local[1]")
    .appName("etl-ventas")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.legacy.javaCharsets", "true")  # permite leer IBM437
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ===============================================================
# 1. EXTRACCIÓN
# ===============================================================
# Todo se lee como texto; los tipos se asignan en el SQL para detectar errores.
# IBM437 es la codificación real del archivo (lee bien Cataluña, Martín, Berguvsvägen).
crudo = spark.read.option("header", True).option("encoding", "IBM437").csv(str(ORIGEN))
filas_entrada = crudo.count()
print(f"Filas de entrada: {filas_entrada}")

# Quitar espacios sobrantes y convertir textos vacíos en nulos
crudo = crudo.select([F.expr(f"NULLIF(REGEXP_REPLACE(TRIM({c}), ' +', ' '), '')").alias(c) for c in crudo.columns])

# ===============================================================
# 2. TRANSFORMACIÓN
# ===============================================================
# Tipos de dato y banderas de calidad (Spark SQL)
crudo.createOrReplaceTempView("ventas_normalizadas")
df = spark.sql(SQL_TIPOS.read_text(encoding="utf-8")).dropDuplicates()

# PRICEEACH viene truncado en 100: se recalcula desde SALES donde no cuadra
precio_mal = F.abs(F.col("sales") - F.col("quantity_ordered") * F.col("price_each")) > 0.01
precios_corregidos = df.filter(precio_mal).count()
df = df.withColumn(
    "price_each",
    F.when(precio_mal, F.round(F.col("sales") / F.col("quantity_ordered"), 2)).otherwise(F.col("price_each")),
)

df = (
    df.replace(CIUDADES, subset=["city"])
    .withColumn("has_quality_error", F.expr(
        "invalid_order_number OR invalid_order_line_number OR invalid_quantity "
        "OR invalid_price_each OR invalid_sales OR invalid_order_date OR invalid_msrp"))
    .withColumn("es_venta_realizada", F.col("status") == "Shipped")
    .withColumn("order_timestamp", F.to_date("order_timestamp"))
    .withColumn("order_date", F.col("order_timestamp"))
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("quarter", F.quarter("order_date"))
    # Métricas nuevas (requerimientos 2 y 9)
    .withColumn("discount_pct", F.round((F.col("msrp") - F.col("price_each")) / F.col("msrp"), 4))
    .withColumn("estimated_cost", F.round(F.col("msrp") * PORC_COSTO * F.col("quantity_ordered"), 2))
    .withColumn("profit", F.round(F.col("sales") - F.col("estimated_cost"), 2))
    .withColumn("margin_pct", F.round(F.col("profit") / F.col("sales"), 4))
)

# Decimales a double para que pandas, JSON y SQLite los manejen sin problema
for nombre, tipo in df.dtypes:
    if tipo.startswith("decimal"):
        df = df.withColumn(nombre, F.col(nombre).cast("double"))

# La transformación ya la hizo Spark. Se pasa a pandas solo para escribir:
# así se evita el requisito de winutils.exe de Spark en Windows.
ventas = df.orderBy("order_number", "order_line_number").toPandas()
spark.stop()

# ===============================================================
# 3. VALIDACIÓN
# ===============================================================
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
RESUMEN.parent.mkdir(parents=True, exist_ok=True)
RESUMEN.write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")

# ===============================================================
# 4. CARGA EN CSV, JSON Y SQL
# ===============================================================
CARPETA_LIMPIA.mkdir(parents=True, exist_ok=True)
ventas.to_csv(CARPETA_LIMPIA / "sales_clean.csv", index=False, encoding="utf-8")
ventas.to_json(CARPETA_LIMPIA / "sales_clean.json", orient="records", indent=2,
               force_ascii=False, date_format="iso")
with sqlite3.connect(CARPETA_LIMPIA / "sales_clean.db") as con:
    ventas.astype({"order_timestamp": str, "order_date": str}).to_sql(
        "ventas", con, if_exists="replace", index=False)

print(f"Filas limpias: {len(ventas)} | Precios corregidos: {precios_corregidos}")
print(f"Ventas Shipped: {resumen['ventas_shipped']:,.2f} en {resumen['pedidos_shipped']} pedidos")
print(f"Archivos creados en: {CARPETA_LIMPIA}")
