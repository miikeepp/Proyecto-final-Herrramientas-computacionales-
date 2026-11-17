"""Primera capa de limpieza para sales_data_sample.csv.

El archivo fuente nunca se modifica. La salida contiene todos los estados de
pedido; las métricas de ventas realizadas deben filtrar es_venta_realizada.
"""

import json
import csv
from collections import defaultdict
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "sales_data_sample.csv"
OUTPUT = ROOT / "data" / "clean" / "sales_clean.jsonl"
CSV_OUTPUT = ROOT / "data" / "clean" / "sales_clean.csv"
VALIDATION = ROOT / "data" / "validation" / "validation_summary.json"
CONFLICTS = ROOT / "data" / "validation" / "duplicate_key_conflicts.jsonl"

SOURCE_COLUMNS = [
    "ORDERNUMBER", "QUANTITYORDERED", "PRICEEACH", "ORDERLINENUMBER",
    "SALES", "ORDERDATE", "STATUS", "QTR_ID", "MONTH_ID", "YEAR_ID",
    "PRODUCTLINE", "MSRP", "PRODUCTCODE", "CUSTOMERNAME", "PHONE",
    "ADDRESSLINE1", "ADDRESSLINE2", "CITY", "STATE", "POSTALCODE",
    "COUNTRY", "TERRITORY", "CONTACTLASTNAME", "CONTACTFIRSTNAME", "DEALSIZE",
]


def nonblank(column_name: str) -> F.Column:
    """Convierte espacios y cadenas vacías a nulo."""
    trimmed = F.trim(F.col(column_name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def invalid(raw: F.Column, parsed: F.Column) -> F.Column:
    return raw.isNotNull() & parsed.isNull()


def write_json_lines(rows: list[str], target: Path) -> None:
    """Escribe JSONL ya materializado desde un DataFrame PySpark."""
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(rows) + ("\n" if rows else ""), encoding="utf-8")


def write_single_json(value: dict, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def write_clean_csv(rows: list[dict], columns: list[str], target: Path) -> None:
    """Escribe la capa limpia con encabezados, UTF-8 y fechas ISO de día."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, delimiter=",")
        writer.writeheader()
        for row in rows:
            csv_row = row.copy()
            for date_column in ("order_date", "order_timestamp"):
                if csv_row.get(date_column) is not None:
                    csv_row[date_column] = str(csv_row[date_column])[:10]
            writer.writerow({column: "" if csv_row.get(column) is None else csv_row[column] for column in columns})


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("sales-cleaning")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Se lee inicialmente como texto para detectar conversiones inválidas.
    raw_schema = T.StructType([T.StructField(name, T.StringType(), True) for name in SOURCE_COLUMNS])
    raw = (
        spark.read.option("header", True).option("encoding", "ISO-8859-1")
        .option("mode", "PERMISSIVE").schema(raw_schema).csv(str(SOURCE))
    )
    input_rows = raw.count()
    print(f"Filas de entrada: {input_rows}", flush=True)
    normalized = raw.select(*[nonblank(name).alias(name) for name in SOURCE_COLUMNS])

    typed = normalized.select(
        F.expr("try_cast(ORDERNUMBER as int)").alias("order_number"),
        F.expr("try_cast(QUANTITYORDERED as int)").alias("quantity_ordered"),
        F.expr("try_cast(PRICEEACH as decimal(12,2))").alias("price_each"),
        F.expr("try_cast(ORDERLINENUMBER as int)").alias("order_line_number"),
        F.expr("try_cast(SALES as decimal(14,2))").alias("sales"),
        F.expr("try_to_timestamp(ORDERDATE, 'M/d/yyyy H:mm')").alias("order_timestamp"),
        F.col("STATUS").alias("status"),
        F.expr("try_cast(QTR_ID as int)").alias("source_quarter_id"),
        F.expr("try_cast(MONTH_ID as int)").alias("source_month_id"),
        F.expr("try_cast(YEAR_ID as int)").alias("source_year_id"),
        F.col("PRODUCTLINE").alias("product_line"),
        F.expr("try_cast(MSRP as decimal(12,2))").alias("msrp"),
        F.col("PRODUCTCODE").alias("product_code"),
        F.col("CUSTOMERNAME").alias("customer_name"),
        F.col("PHONE").alias("phone"), F.col("ADDRESSLINE1").alias("address_line_1"),
        F.col("ADDRESSLINE2").alias("address_line_2"), F.col("CITY").alias("city"),
        F.col("STATE").alias("state"), F.col("POSTALCODE").alias("postal_code"),
        F.col("COUNTRY").alias("country"), F.col("TERRITORY").alias("territory"),
        F.col("CONTACTLASTNAME").alias("contact_last_name"),
        F.col("CONTACTFIRSTNAME").alias("contact_first_name"), F.col("DEALSIZE").alias("deal_size"),
        invalid(F.col("ORDERNUMBER"), F.expr("try_cast(ORDERNUMBER as int)")).alias("invalid_order_number"),
        invalid(F.col("ORDERLINENUMBER"), F.expr("try_cast(ORDERLINENUMBER as int)")).alias("invalid_order_line_number"),
        invalid(F.col("QUANTITYORDERED"), F.expr("try_cast(QUANTITYORDERED as int)")).alias("invalid_quantity"),
        invalid(F.col("PRICEEACH"), F.expr("try_cast(PRICEEACH as decimal(12,2))")).alias("invalid_price_each"),
        invalid(F.col("SALES"), F.expr("try_cast(SALES as decimal(14,2))")).alias("invalid_sales"),
        invalid(F.col("ORDERDATE"), F.expr("try_to_timestamp(ORDERDATE, 'M/d/yyyy H:mm')")).alias("invalid_order_date"),
        invalid(F.col("MSRP"), F.expr("try_cast(MSRP as decimal(12,2))")).alias("invalid_msrp"),
    )

    clean_with_flags = typed.withColumn(
        "has_quality_error", F.expr("invalid_order_number OR invalid_order_line_number OR invalid_quantity OR invalid_price_each OR invalid_sales OR invalid_order_date OR invalid_msrp")
    )
    clean_with_flags = clean_with_flags.withColumn("es_venta_realizada", F.col("status") == F.lit("Shipped"))
    clean_with_flags = clean_with_flags.withColumn("order_date", F.to_date("order_timestamp"))
    clean_with_flags = clean_with_flags.withColumn("year", F.year("order_timestamp"))
    clean_with_flags = clean_with_flags.withColumn("month", F.month("order_timestamp"))
    clean_with_flags = clean_with_flags.withColumn("quarter", F.quarter("order_timestamp"))

    # La transformación anterior se ejecuta en PySpark. La materialización local
    # evita el requisito winutils.exe del escritor Hadoop en este Windows.
    clean_columns = clean_with_flags.columns
    records = [json.loads(line) for line in clean_with_flags.toJSON().collect()]
    for record in records:
        for column in clean_columns:
            record.setdefault(column, None)
    records_by_key = defaultdict(list)
    for record in records:
        records_by_key[(record.get("order_number"), record.get("order_line_number"))].append(record)

    conflicts = []
    for (order_number, order_line_number), rows in records_by_key.items():
        payloads = {json.dumps(row, sort_keys=True, default=str) for row in rows}
        if len(rows) > 1 and len(payloads) > 1:
            conflicts.append({
                "order_number": order_number,
                "order_line_number": order_line_number,
                "rows_with_key": len(rows),
                "distinct_payloads": len(payloads),
            })
    write_json_lines([json.dumps(row, default=str) for row in conflicts], CONFLICTS)
    print("Reporte de conflictos de clave creado.", flush=True)

    # Solo se quitan filas exactamente duplicadas; conflictos de clave se preservan.
    unique_records = []
    seen_records = set()
    for record in records:
        fingerprint = json.dumps(record, sort_keys=True, default=str)
        if fingerprint not in seen_records:
            seen_records.add(fingerprint)
            unique_records.append(record)
    output_rows = len(unique_records)
    write_json_lines([json.dumps(row, default=str) for row in unique_records], OUTPUT)
    write_clean_csv(unique_records, clean_columns, CSV_OUTPUT)
    print(f"Capa limpia creada: {output_rows} filas.", flush=True)

    timestamps = [row["order_timestamp"] for row in records if row.get("order_timestamp") is not None]
    summary = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "exact_duplicates_removed": input_rows - output_rows,
        "duplicate_key_conflicts": len(conflicts),
        "sales_scope": "Solo STATUS = 'Shipped' para métricas principales; todas las filas se conservan.",
        "min_order_timestamp": min(timestamps) if timestamps else None,
        "max_order_timestamp": max(timestamps) if timestamps else None,
        "non_positive_quantity": sum(row.get("quantity_ordered") is not None and row["quantity_ordered"] <= 0 for row in records),
        "negative_sales": sum(row.get("sales") is not None and row["sales"] < 0 for row in records),
    }
    for column in clean_columns:
        summary["null_" + column] = sum(row.get(column) is None for row in records)
    for column in ["invalid_order_number", "invalid_order_line_number", "invalid_quantity", "invalid_price_each", "invalid_sales", "invalid_order_date", "invalid_msrp", "has_quality_error"]:
        summary[column] = sum(bool(row.get(column)) for row in records)
    write_single_json(summary, VALIDATION)
    print("Resumen de validación creado.", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
