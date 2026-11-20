# ETL de ventas — capa limpia

Ejecutar desde la raíz del proyecto:

```powershell
python etl_pyspark.py
```

El proceso lee `sales_data_sample.csv` sin modificarlo y crea:

- `data/clean/sales_clean.jsonl`: dataset limpio en JSON Lines.
- `data/clean/sales_clean.csv`: la misma capa limpia en CSV UTF-8, delimitado por comas y con fechas `yyyy-MM-dd`.
- `data/validation/validation_summary.json`: conteos y controles de validación.
- `data/validation/duplicate_key_conflicts.jsonl`: posibles claves `(ORDERNUMBER, ORDERLINENUMBER)` repetidas con contenido diferente. Estas filas no se eliminan.

## Tecnologías utilizadas

- **Python + PySpark:** orquesta la lectura, normalización, validación y generación de archivos.
- **Spark SQL:** las conversiones y las banderas de calidad están en `transformaciones_etl.sql`, ejecutado por PySpark sobre una vista temporal.
- **Power Query:** `power_query/sales_clean.pq` carga el CSV limpio, asigna tipos para análisis y conserva únicamente las ventas realizadas. En Power BI o Excel, abra el Editor avanzado, pegue el contenido y reemplace `RutaArchivo` por la ruta absoluta de este proyecto. Ejecute antes `python etl_pyspark.py` para actualizar el CSV.

La salida se entrega como JSONL porque el entorno Windows disponible no incluye `winutils.exe`, requisito del escritor Parquet de Spark. Las transformaciones y validaciones se ejecutan con PySpark; al instalar Hadoop/winutils compatible se puede cambiar el destino a Parquet sin alterar las reglas.

## Regla de ventas realizadas

Todas las filas, incluidos todos los estados de pedido, se conservan. La columna `es_venta_realizada` es verdadera únicamente cuando `status = 'Shipped'`. Las métricas principales de ventas deben filtrar dicha bandera.

## Reglas aplicadas

- Se lee el CSV con `ISO-8859-1`, compatible con la codificación del archivo fuente.
- Se eliminan espacios externos y las cadenas vacías se convierten en nulos.
- Se convierten tipos con `try_cast`; valores que no puedan convertirse permanecen nulos y se señalan en las columnas `invalid_*` y `has_quality_error`.
- Se derivan fecha, año, mes y trimestre de `ORDERDATE`.
- Solo se eliminan duplicados exactos. Una colisión de clave con datos distintos se reporta y permanece en la salida.
- `SALES` es el importe oficial: no se recalcula a partir de cantidad y precio unitario.

## Limitaciones conocidas

- No hay costo, margen ni utilidad: no se puede calcular rentabilidad real por categoría.
- No hay canal de venta: no se pueden comparar productos por canal.
- No hay descuento explícito. `MSRP` y `PRICEEACH` no se usan para inferirlo porque no son consistentes de forma confiable con `SALES`.
- No hay coordenadas geográficas: el mapa puede agregarse por país, o requerirá una tabla geográfica externa y documentada.
