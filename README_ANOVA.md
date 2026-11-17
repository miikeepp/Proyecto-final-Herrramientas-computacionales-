# Análisis ANOVA por cliente

El análisis usa `data/clean/sales_clean.csv` sin modificarlo. Filtra pedidos
`Shipped`, suma las líneas por pedido y calcula un promedio de ventas por pedido
para cada cliente. Cada cliente aporta una única observación a su región.

## Dependencias

```powershell
python -m pip install -r requirements_anova.txt
```

## Ejecución

```powershell
python analisis_anova.py
```

El script valida que cada pedido y cliente tengan una única región. Escribe los
datos a nivel cliente, tablas descriptivas, diagnósticos, resultados de Levene,
ANOVA clásico, eta cuadrado y Welch ANOVA en `data/analysis/anova/`, junto con
histogramas, un diagrama de caja y gráficos Q–Q.

Los resultados se calculan desde el CSV limpio; no están codificados como
constantes en el script.
