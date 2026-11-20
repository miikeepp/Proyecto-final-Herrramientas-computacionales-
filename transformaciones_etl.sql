-- Se ejecuta desde etl_pyspark.py sobre la vista temporal ventas_normalizadas.
-- Spark SQL conserva los nulos cuando una conversión no es válida.
SELECT
    TRY_CAST(ORDERNUMBER AS INT) AS order_number,
    TRY_CAST(QUANTITYORDERED AS INT) AS quantity_ordered,
    TRY_CAST(PRICEEACH AS DECIMAL(12,2)) AS price_each,
    TRY_CAST(ORDERLINENUMBER AS INT) AS order_line_number,
    TRY_CAST(SALES AS DECIMAL(14,2)) AS sales,
    TRY_TO_TIMESTAMP(ORDERDATE, 'M/d/yyyy H:mm') AS order_timestamp,
    STATUS AS status,
    TRY_CAST(QTR_ID AS INT) AS source_quarter_id,
    TRY_CAST(MONTH_ID AS INT) AS source_month_id,
    TRY_CAST(YEAR_ID AS INT) AS source_year_id,
    PRODUCTLINE AS product_line, TRY_CAST(MSRP AS DECIMAL(12,2)) AS msrp,
    PRODUCTCODE AS product_code, CUSTOMERNAME AS customer_name, PHONE AS phone,
    ADDRESSLINE1 AS address_line_1, ADDRESSLINE2 AS address_line_2, CITY AS city,
    STATE AS state, POSTALCODE AS postal_code, COUNTRY AS country, TERRITORY AS territory,
    CONTACTLASTNAME AS contact_last_name, CONTACTFIRSTNAME AS contact_first_name,
    DEALSIZE AS deal_size,
    ORDERNUMBER IS NOT NULL AND TRY_CAST(ORDERNUMBER AS INT) IS NULL AS invalid_order_number,
    ORDERLINENUMBER IS NOT NULL AND TRY_CAST(ORDERLINENUMBER AS INT) IS NULL AS invalid_order_line_number,
    QUANTITYORDERED IS NOT NULL AND TRY_CAST(QUANTITYORDERED AS INT) IS NULL AS invalid_quantity,
    PRICEEACH IS NOT NULL AND TRY_CAST(PRICEEACH AS DECIMAL(12,2)) IS NULL AS invalid_price_each,
    SALES IS NOT NULL AND TRY_CAST(SALES AS DECIMAL(14,2)) IS NULL AS invalid_sales,
    ORDERDATE IS NOT NULL AND TRY_TO_TIMESTAMP(ORDERDATE, 'M/d/yyyy H:mm') IS NULL AS invalid_order_date,
    MSRP IS NOT NULL AND TRY_CAST(MSRP AS DECIMAL(12,2)) IS NULL AS invalid_msrp
FROM ventas_normalizadas;
