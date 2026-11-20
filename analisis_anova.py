"""ANOVA: ¿el importe promedio por pedido de los clientes cambia según la región?

Cada cliente aporta una sola observación (su promedio de ventas por pedido),
así las observaciones son independientes entre sí.
"""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # guarda las gráficas sin abrir ventanas
import matplotlib.pyplot as plt
import pandas as pd
from scipy import stats

RAIZ = Path(__file__).resolve().parent
ORIGEN = RAIZ / "data" / "clean" / "sales_clean.csv"
SALIDA = RAIZ / "data" / "analysis" / "anova"
SALIDA.mkdir(parents=True, exist_ok=True)
ALFA = 0.05

# ===============================================================
# 1. PREPARAR LOS DATOS
# ===============================================================
# keep_default_na=False: el territorio "NA" (Norteamérica) no es un nulo
ventas = pd.read_csv(ORIGEN, keep_default_na=False, na_values=[""])
ventas = ventas[ventas["status"] == "Shipped"]

# Total de cada pedido
pedidos = ventas.groupby(["order_number", "customer_name", "territory"], as_index=False)["sales"].sum()
assert pedidos["order_number"].is_unique, "Un pedido tiene más de un cliente o región"

# Promedio por pedido de cada cliente
clientes = pedidos.groupby(["customer_name", "territory"], as_index=False).agg(
    orders=("order_number", "count"),
    avg_sales_per_order=("sales", "mean"),
)
assert clientes["customer_name"].is_unique, "Un cliente aparece en más de una región"
clientes.to_csv(SALIDA / "customer_level_data.csv", index=False)

grupos = {region: datos["avg_sales_per_order"] for region, datos in clientes.groupby("territory")}
print(f"Pedidos: {len(pedidos)} | Clientes: {len(clientes)} | Regiones: {list(grupos)}")

# ===============================================================
# 2. DESCRIPTIVOS Y SUPUESTOS
# ===============================================================
descriptivos = clientes.groupby("territory")["avg_sales_per_order"].agg(
    customers="count", mean_avg_sales_per_order="mean", stddev_sample="std")
descriptivos.to_csv(SALIDA / "descriptive_by_region.csv")

# Normalidad por región (Shapiro-Wilk)
normalidad = pd.DataFrame(
    [(region, *stats.shapiro(valores)) for region, valores in grupos.items()],
    columns=["territory", "shapiro_w", "shapiro_p_value"],
)
normalidad.to_csv(SALIDA / "diagnostics.csv", index=False)

# ===============================================================
# 3. PRUEBAS
# ===============================================================
levene = stats.levene(*grupos.values(), center="median")    # igualdad de varianzas
clasico = stats.f_oneway(*grupos.values())                   # ANOVA clásico
welch = stats.f_oneway(*grupos.values(), equal_var=False)    # ANOVA de Welch

# Tamaño del efecto: eta cuadrado = SC entre grupos / SC total
media_total = clientes["avg_sales_per_order"].mean()
sc_entre = sum(len(v) * (v.mean() - media_total) ** 2 for v in grupos.values())
sc_total = ((clientes["avg_sales_per_order"] - media_total) ** 2).sum()
eta2 = sc_entre / sc_total

resultados = pd.DataFrame([
    ["Levene_median", levene.statistic, levene.pvalue, None],
    ["Classic_ANOVA", clasico.statistic, clasico.pvalue, eta2],
    ["Welch_ANOVA", welch.statistic, welch.pvalue, None],
], columns=["test", "statistic", "p_value", "effect_eta_squared"])
resultados.to_csv(SALIDA / "anova_results.csv", index=False)

# Tukey solo si el ANOVA encuentra diferencias
if clasico.pvalue < ALFA:
    tukey = stats.tukey_hsd(*grupos.values())
    nombres = list(grupos)
    pares = [
        (nombres[i], nombres[j], tukey.statistic[i, j], tukey.pvalue[i, j])
        for i in range(len(nombres)) for j in range(i + 1, len(nombres))
    ]
    pd.DataFrame(pares, columns=["region_a", "region_b", "difference_of_means", "p_value_adjusted"]) \
        .to_csv(SALIDA / "tukey_hsd_results.csv", index=False)
    decision = "se rechaza H0. Al menos una región es diferente; ver tukey_hsd_results.csv."
else:
    decision = ("no se rechaza H0. No hay evidencia suficiente de diferencias "
                "entre regiones; Tukey HSD no se aplica.")

conclusion = (
    "H0: el importe promedio por pedido de los clientes es igual en todas las regiones.\n"
    "H1: al menos una región tiene un importe promedio por pedido diferente.\n"
    f"Nivel de significancia: alfa = {ALFA}\n"
    f"Levene: p = {levene.pvalue:.4f} (varianzas {'iguales' if levene.pvalue >= ALFA else 'distintas'})\n"
    f"ANOVA clásico: F = {clasico.statistic:.4f}; p = {clasico.pvalue:.4f}; eta² = {eta2:.4f}\n"
    f"ANOVA de Welch: F = {welch.statistic:.4f}; p = {welch.pvalue:.4f}\n"
    f"Decisión: {decision}\n"
)
(SALIDA / "anova_conclusion.txt").write_text(conclusion, encoding="utf-8")
print(conclusion)

# ===============================================================
# 4. GRÁFICAS
# ===============================================================
nombres = list(grupos)

plt.figure(figsize=(8, 5))
plt.boxplot(list(grupos.values()), tick_labels=nombres)
plt.title("Importe promedio por pedido de cada cliente, por región")
plt.ylabel("Ventas promedio por pedido")
plt.tight_layout()
plt.savefig(SALIDA / "boxplot_by_region.png", dpi=150)
plt.close()

fig, ejes = plt.subplots(1, len(nombres), figsize=(4 * len(nombres), 4))
for eje, (region, valores) in zip(ejes, grupos.items()):
    eje.hist(valores, bins=8, edgecolor="black")
    eje.set_title(f"{region} (n = {len(valores)})")
fig.suptitle("Distribución por región")
fig.tight_layout()
fig.savefig(SALIDA / "histograms_by_region.png", dpi=150)
plt.close(fig)

fig, ejes = plt.subplots(1, len(nombres), figsize=(4 * len(nombres), 4))
for eje, (region, valores) in zip(ejes, grupos.items()):
    stats.probplot(valores, dist="norm", plot=eje)
    eje.set_title(f"Q-Q {region}")
fig.tight_layout()
fig.savefig(SALIDA / "qq_plots_by_region.png", dpi=150)
plt.close(fig)

print(f"Resultados guardados en: {SALIDA}")
