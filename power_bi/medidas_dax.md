# Medidas DAX del tablero

Todas las medidas de ventas usan solo pedidos `Shipped` (ventas realizadas).

## 1. Medidas que ya tiene el `.pbix`

| Medida | Fórmula | Requerimiento |
|---|---|---|
| Ventas realizadas | `CALCULATE(SUM(sales_clean[sales]), sales_clean[status] = "Shipped")` | Base de todo |
| Ventas mes anterior | `CALCULATE([Ventas realizadas], DATEADD(Calendario[Date], -1, MONTH))` | 5 |
| Crecimiento mensual % | `DIVIDE([Ventas realizadas] - [Ventas mes anterior], [Ventas mes anterior])` | 5 |
| Ventas enero a mayo | `CALCULATE([Ventas realizadas], Calendario[MesNumero] <= 5)` | 4 |
| Crecimiento enero a mayo % | Ventas enero a mayo vs. el mismo periodo del año anterior (`SAMEPERIODLASTYEAR`) | 4 |
| Promedio venta por línea | `CALCULATE(AVERAGE(sales_clean[sales]), sales_clean[status] = "Shipped")` | 6 |
| Unidades vendidas | `CALCULATE(SUM(sales_clean[quantity_ordered]), sales_clean[status] = "Shipped")` | 7 |
| Ranking cliente | `RANKX(ALLSELECTED(sales_clean[customer_name]), [Ventas realizadas], , DESC, DENSE)` | 3 |
| Ventas acumuladas clientes | Suma de las ventas de los clientes con ventas ≥ al cliente actual | 10 |
| Porcentaje acumulado clientes | `DIVIDE([Ventas acumuladas clientes], total de ventas)` | 10 |
| Clasificación ABC | A ≤ 80 %, B ≤ 95 %, C el resto | 10 |
| Pedidos realizados | `CALCULATE(DISTINCTCOUNT(sales_clean[order_number]), sales_clean[status] = "Shipped")` | 8 |
| Ticket promedio | `DIVIDE([Ventas realizadas], [Pedidos realizados])` | 8 |
| Ventas YTD / MTD | `TOTALYTD(...)` / `TOTALMTD(...)` | 8 |

> La comparación año a año usa **enero a mayo** porque 2005 solo tiene datos hasta mayo. Así se comparan periodos equivalentes.

## 2. Medidas nuevas

Antes, actualice la consulta `sales_clean` con [`power_query/sales_clean.pq`](../power_query/sales_clean.pq) para que aparezcan las columnas `discount_pct`, `estimated_cost`, `profit` y `margin_pct`.

Créelas en **Modelado → Nueva medida**, dentro de la tabla `sales_clean`.

### Requerimiento 2: rentabilidad por categoría

```DAX
Ganancia estimada =
CALCULATE(
    SUM(sales_clean[profit]),
    sales_clean[status] = "Shipped"
)
```

```DAX
Margen estimado % =
DIVIDE([Ganancia estimada], [Ventas realizadas])
```

**Visual:** gráfico de barras con `product_line` en el eje, `Ganancia estimada` como valor y `Margen estimado %` en la información sobre herramientas.

> El dataset no trae costos. Se supone un costo del 60 % del precio de lista (MSRP). Es un supuesto del proyecto y se puede cambiar en `PORC_COSTO` dentro de `etl_pyspark.py`.

### Requerimiento 7: productos más vendidos por canal

El dataset no trae canal de venta. Como aproximación se usa `deal_size` (tamaño del negocio: Small, Medium, Large).

```DAX
Unidades Small = CALCULATE([Unidades vendidas], sales_clean[deal_size] = "Small")
Unidades Medium = CALCULATE([Unidades vendidas], sales_clean[deal_size] = "Medium")
Unidades Large = CALCULATE([Unidades vendidas], sales_clean[deal_size] = "Large")
```

**Visual:** matriz con `product_line` en filas y `Unidades vendidas`, `Unidades Small`, `Unidades Medium` y `Unidades Large` como valores.

> Estas medidas y visuales ya están en la página **Rentabilidad y descuentos** del `.pbix`.

### Requerimiento 9: descuentos y volumen de compra

```DAX
Descuento promedio % =
VAR PrecioLista =
    CALCULATE(
        SUMX(sales_clean, sales_clean[msrp] * sales_clean[quantity_ordered]),
        sales_clean[status] = "Shipped"
    )
RETURN
    DIVIDE(PrecioLista - [Ventas realizadas], PrecioLista)
```

Columna calculada (**Modelado → Nueva columna**) para agrupar por rango de descuento:

```DAX
Rango descuento =
SWITCH(
    TRUE(),
    sales_clean[discount_pct] < 0, "1. Sobre precio de lista",
    sales_clean[discount_pct] < 0.10, "2. 0 a 10 %",
    sales_clean[discount_pct] < 0.20, "3. 10 a 20 %",
    "4. Más de 20 %"
)
```

```DAX
Unidades por línea =
DIVIDE([Unidades vendidas], [Líneas vendidas])
```

**Visuales:**
- Columnas con `Rango descuento` en el eje y `Unidades por línea` como valor.
- Dispersión con `product_code` como detalle, `Descuento promedio %` en X y `Unidades vendidas` en Y.

Un descuento negativo significa que el producto se vendió por encima del precio de lista.
