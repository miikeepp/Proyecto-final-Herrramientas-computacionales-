# Análisis de ventas de un retail multinacional

Proyecto final de Herramientas Computacionales. Recorre el ciclo completo del dato:

**ETL con PySpark → carga en CSV, JSON y SQL → tablero en Power BI con DAX → ANOVA**

Dataset: [Sample Sales Data (Kaggle)](https://www.kaggle.com/datasets/kyanyoga/sample-sales-data), con 2.823 líneas de pedido entre enero de 2003 y mayo de 2005.

## Estructura

```text
├── sales_data_sample.csv          # datos originales (no se modifican)
├── etl_pyspark.py                 # ETL: extracción, limpieza, validación y carga
├── transformaciones_etl.sql       # tipos de dato y banderas de calidad (Spark SQL)
├── analisis_anova.py              # ANOVA por región
├── requirements.txt
├── taller final.pbix              # tablero de Power BI
├── power_query/sales_clean.pq     # consulta M de Power BI
├── power_bi/medidas_dax.md        # medidas DAX por requerimiento
├── Presentacion Herramientas computacionales/index.html
└── data/
    ├── clean/                     # sales_clean.csv, .json y .db
    ├── validation/                # validation_summary.json
    └── analysis/anova/            # resultados y gráficas del ANOVA
```

## Cómo ejecutarlo

Requisitos: Python 3.11 o superior y Java 17 o superior (lo necesita Spark).

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python etl_pyspark.py
python analisis_anova.py
```

Después abra `taller final.pbix` y haga clic en **Actualizar**. Si es la primera vez en su PC, pegue antes la consulta de `power_query/sales_clean.pq` (ver [Power BI](#power-bi)).

## ETL

### Extracción
`etl_pyspark.py` lee el CSV original con Spark. Todas las columnas se leen como texto, para que los errores de conversión se detecten en el siguiente paso en lugar de perderse.

### Limpieza y transformación

| Problema en el original | Solución |
|---|---|
| Tildes mal leídas (`Mart¡n`, `Catalu¤a`) | El archivo se lee con su codificación real, IBM437 |
| Espacios sobrantes y textos vacíos | Se recortan los espacios y los textos vacíos pasan a nulo |
| Tipos de dato | `transformaciones_etl.sql` los convierte con `TRY_CAST` y marca cada fallo con una bandera `invalid_*` |
| `PRICEEACH` truncado en 100 (1.304 filas) | Se recalcula como `SALES / QUANTITYORDERED` |
| Ciudades mal escritas (`Gensve`, `Koln`, `NYC`…) | Se corrigen con un diccionario |
| Duplicados exactos | Se eliminan (no había ninguno) |

Además se crean estas columnas:
- **Fecha:** `order_date`, `year`, `month` y `quarter`.
- **Bandera de venta realizada:** `es_venta_realizada`, que es verdadera solo cuando `status = Shipped`.
- **Métricas nuevas:**
  - `discount_pct` = (MSRP − precio) / MSRP
  - `estimated_cost` = MSRP × 60 % × cantidad
  - `profit` = ventas − costo
  - `margin_pct` = ganancia / ventas

### Validación
Si alguna de estas condiciones falla, el script se detiene:
- no hay líneas de pedido repetidas;
- no hay errores de conversión;
- `SALES` cuadra con cantidad × precio.

El resumen se guarda en `data/validation/validation_summary.json`.

### Carga

| Archivo | Formato | Uso |
|---|---|---|
| `data/clean/sales_clean.csv` | CSV | Fuente del tablero de Power BI |
| `data/clean/sales_clean.json` | JSON | Intercambio con otras aplicaciones |
| `data/clean/sales_clean.db` | SQLite (tabla `ventas`) | Consultas SQL |

> Spark hace todas las transformaciones. La escritura final usa pandas porque en Windows el escritor de Spark exige `winutils.exe`.

## Power BI

`taller final.pbix` tiene 7 páginas. Las medidas DAX de cada requerimiento están en [`power_bi/medidas_dax.md`](power_bi/medidas_dax.md).

Para que el tablero cargue los datos nuevos:
1. Vaya a **Transformar datos**, haga clic derecho en `sales_clean` y elija **Editor avanzado**.
2. Pegue el contenido de `power_query/sales_clean.pq` y cambie `RutaProyecto` por la carpeta del proyecto.
3. Haga clic en **Cerrar y aplicar**.

| # | Requerimiento | Dónde está |
|---|---|---|
| 1 | Ventas mensuales por país | Página 1: líneas por `AñoMes` y país |
| 2 | Rentabilidad por categoría | Medidas `Ganancia estimada` y `Margen estimado %` |
| 3 | Clientes con mayores ingresos | Página 1: barras por cliente |
| 4 | Comparativa año a año | Comparación anual (enero a mayo) |
| 5 | Crecimiento mensual | Página 1: `Crecimiento mensual %` |
| 6 | Promedio de ventas por región | Productos y regiones |
| 7 | Productos más vendidos por canal | Productos y regiones, con `deal_size` como canal |
| 8 | Mapa geográfico | Mapa de ventas |
| 9 | Descuentos y volumen de compra | Precio y cantidad, con `Rango descuento` |
| 10 | Análisis ABC de clientes | Clientes ABC |

## ANOVA

`analisis_anova.py` responde esta pregunta: **¿el importe promedio por pedido de los clientes cambia según la región (APAC, EMEA, Japan, NA)?**

- Solo se usan pedidos `Shipped`. Cada cliente aporta una observación, su promedio por pedido, para que las observaciones sean independientes.
- Supuestos: normalidad con Shapiro-Wilk por región y varianzas con Levene.
- Pruebas: ANOVA clásico, ANOVA de Welch y tamaño del efecto (η²). Tukey HSD solo se aplica si p < 0,05.

| Prueba | Resultado |
|---|---|
| Levene | p = 0,202 → varianzas similares |
| ANOVA clásico | F = 0,485; p = 0,694; η² = 0,016 |
| ANOVA de Welch | F = 0,477; p = 0,702 |

**Conclusión:** con α = 0,05 no se rechaza H₀. No hay evidencia de que el importe promedio por pedido cambie entre regiones. APAC (6 clientes) y Japan (4) tienen muestras pequeñas, así que el resultado se debe interpretar con cautela.

## Resultados principales (pedidos Shipped)

- **Volumen:** 9,29 millones en ventas, 286 pedidos y 91.403 unidades.
- **Categorías:** Classic Cars es la que más vende y más gana, pero tiene el margen más bajo (37 %). Trains tiene el margen más alto (48 %).
- **Descuentos:** el descuento no se relaciona con el volumen. Cada línea de pedido tiene unas 35 unidades en cualquier rango de descuento (correlación de Spearman = 0,03).
- **Regiones:** el ticket promedio por cliente es similar en todas las regiones (ANOVA).

## Limitaciones

- **Costo:** el dataset no trae costos. La ganancia supone un costo del 60 % del MSRP (`PORC_COSTO` en el ETL).
- **Canal:** no hay canal de venta. Se usa `deal_size` como aproximación.
- **Periodo:** 2005 solo tiene datos de enero a mayo, por eso la comparación anual usa ese mismo periodo.
- **Territorio:** `territory` es la oficina comercial que atiende al cliente, no su ubicación geográfica. Por eso Filipinas y parte de Singapur aparecen en "Japan".
- **Moneda:** no está documentada en el dataset.
