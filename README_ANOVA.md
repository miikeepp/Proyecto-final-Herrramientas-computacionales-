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

## Hipótesis y decisión

- **H₀:** el importe promedio por pedido de los clientes es igual en todas las regiones analizadas.
- **H₁:** al menos una región tiene un importe promedio por pedido diferente.
- **Nivel de significancia:** α = 0,05.

El script escribe `anova_conclusion.txt` con la decisión basada en el p-valor del
ANOVA clásico. Solo si `p < 0,05`, genera `tukey_hsd_results.csv`: allí aparecen
las comparaciones por pares de regiones y sus p-valores ajustados. Si `p ≥ 0,05`,
no se ejecuta Tukey porque no hay evidencia global suficiente para buscar pares
que difieran.

Los resultados se calculan desde el CSV limpio; no están codificados como
constantes en el script.
