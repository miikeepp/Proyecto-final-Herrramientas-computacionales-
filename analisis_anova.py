"""ANOVA de promedios de ventas por pedido, con una observación por cliente."""
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from scipy import stats


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "data" / "clean" / "sales_clean.csv"
OUTPUT = ROOT / "data" / "analysis" / "anova"
ALPHA = 0.05
H0 = "El importe promedio por pedido de los clientes es igual en todas las regiones analizadas."
H1 = "Al menos una región tiene un importe promedio por pedido de los clientes diferente."


def write_csv(path, rows, fieldnames):
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_text(path, content):
    path.write_text(content + "\n", encoding="utf-8")


def welch_anova(groups):
    """Welch ANOVA con aproximación de Welch-Satterthwaite."""
    names = list(groups)
    n = {name: len(groups[name]) for name in names}
    mean = {name: sum(groups[name]) / n[name] for name in names}
    variance = {
        name: sum((value - mean[name]) ** 2 for value in groups[name]) / (n[name] - 1)
        for name in names
    }
    k = len(names)
    weights = {name: n[name] / variance[name] for name in names}
    weight_sum = sum(weights.values())
    weighted_mean = sum(weights[name] * mean[name] for name in names) / weight_sum
    a = sum(weights[name] * (mean[name] - weighted_mean) ** 2 for name in names) / (k - 1)
    b = sum((1 / (n[name] - 1)) * (1 - weights[name] / weight_sum) ** 2 for name in names)
    statistic = a / (1 + (2 * (k - 2) / (k**2 - 1)) * b)
    df1 = k - 1
    df2 = (k**2 - 1) / (3 * b)
    return statistic, df1, df2, stats.f.sf(statistic, df1, df2)


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with SOURCE.open(encoding="utf-8", newline="") as file:
        shipped_rows = [row for row in csv.DictReader(file) if row["status"] == "Shipped"]

    orders = {}
    for row in shipped_rows:
        order = orders.setdefault(row["order_number"], {"sales": 0.0, "territories": set(), "customers": set()})
        order["sales"] += float(row["sales"])
        order["territories"].add(row["territory"])
        order["customers"].add(row["customer_name"])

    invalid_orders = [number for number, order in orders.items() if len(order["territories"]) != 1 or len(order["customers"]) != 1]
    if invalid_orders:
        raise ValueError(f"Hay {len(invalid_orders)} pedidos sin región o cliente único.")

    clients = defaultdict(lambda: {"territories": set(), "order_sales": []})
    for order in orders.values():
        customer = next(iter(order["customers"]))
        clients[customer]["territories"].update(order["territories"])
        clients[customer]["order_sales"].append(order["sales"])

    invalid_clients = [name for name, client in clients.items() if len(client["territories"]) != 1]
    if invalid_clients:
        raise ValueError(f"Hay {len(invalid_clients)} clientes con región no única.")

    customer_rows = []
    groups = defaultdict(list)
    for customer, client in clients.items():
        territory = next(iter(client["territories"]))
        average = sum(client["order_sales"]) / len(client["order_sales"])
        customer_rows.append({"customer_name": customer, "territory": territory, "orders": len(client["order_sales"]), "avg_sales_per_order": average})
        groups[territory].append(average)
    groups = dict(sorted(groups.items()))

    descriptive, diagnostics = [], []
    for territory, values in groups.items():
        descriptive.append({
            "territory": territory, "customers": len(values),
            "mean_avg_sales_per_order": sum(values) / len(values),
            "stddev_sample": stats.tstd(values),
        })
        shapiro_stat, shapiro_p = stats.shapiro(values)
        diagnostics.append({"territory": territory, "shapiro_w": shapiro_stat, "shapiro_p_value": shapiro_p})

    levene_stat, levene_p = stats.levene(*groups.values(), center="median")
    classic_stat, classic_p = stats.f_oneway(*groups.values())
    all_values = [value for values in groups.values() for value in values]
    grand_mean = sum(all_values) / len(all_values)
    ss_between = sum(len(values) * (sum(values) / len(values) - grand_mean) ** 2 for values in groups.values())
    ss_total = sum((value - grand_mean) ** 2 for value in all_values)
    eta_squared = ss_between / ss_total
    welch_stat, welch_df1, welch_df2, welch_p = welch_anova(groups)

    # Tukey se aplica únicamente tras rechazar H0 con el ANOVA clásico.
    # Identifica cuáles pares de regiones explican la diferencia global.
    tukey_rows = []
    if classic_p < ALPHA:
        tukey = stats.tukey_hsd(*groups.values())
        names = list(groups)
        for left_index, left_name in enumerate(names):
            for right_index in range(left_index + 1, len(names)):
                tukey_rows.append({
                    "region_1": left_name,
                    "region_2": names[right_index],
                    "difference_of_means": tukey.statistic[left_index, right_index],
                    "p_value_adjusted": tukey.pvalue[left_index, right_index],
                    "significant_at_alpha": tukey.pvalue[left_index, right_index] < ALPHA,
                })

    write_csv(OUTPUT / "customer_level_data.csv", customer_rows, ["customer_name", "territory", "orders", "avg_sales_per_order"])
    write_csv(OUTPUT / "descriptive_by_region.csv", descriptive, list(descriptive[0]))
    write_csv(OUTPUT / "diagnostics.csv", diagnostics, list(diagnostics[0]))
    results = [
        {"test": "Levene_median", "statistic": levene_stat, "df1": len(groups) - 1, "df2": len(all_values) - len(groups), "p_value": levene_p, "effect_eta_squared": ""},
        {"test": "Classic_ANOVA", "statistic": classic_stat, "df1": len(groups) - 1, "df2": len(all_values) - len(groups), "p_value": classic_p, "effect_eta_squared": eta_squared},
        {"test": "Welch_ANOVA", "statistic": welch_stat, "df1": welch_df1, "df2": welch_df2, "p_value": welch_p, "effect_eta_squared": ""},
    ]
    write_csv(OUTPUT / "anova_results.csv", results, list(results[0]))
    if tukey_rows:
        write_csv(OUTPUT / "tukey_hsd_results.csv", tukey_rows, list(tukey_rows[0]))

    conclusion = (
        f"Hipótesis nula (H0): {H0}\n"
        f"Hipótesis alternativa (H1): {H1}\n"
        f"Nivel de significancia: α = {ALPHA:.2f}\n"
        f"ANOVA clásico: F = {classic_stat:.4f}; p = {classic_p:.4f}.\n"
    )
    if classic_p < ALPHA:
        conclusion += (
            "Decisión: se rechaza H0. Hay evidencia de que al menos una media regional difiere.\n"
            "Se generó tukey_hsd_results.csv para identificar los pares de regiones diferentes."
        )
    else:
        conclusion += (
            "Decisión: no se rechaza H0. No hay evidencia suficiente para afirmar "
            "diferencias entre las medias regionales; Tukey HSD no se aplica."
        )
    write_text(OUTPUT / "anova_conclusion.txt", conclusion)

    names, values = list(groups), list(groups.values())
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    for axis, name, value in zip(axes.flat, names, values):
        axis.hist(value, bins="auto", color="#4c78a8", edgecolor="white")
        axis.set_title(name); axis.set_xlabel("Promedio de ventas por pedido"); axis.set_ylabel("Clientes")
    fig.suptitle("Histogramas por región")
    fig.savefig(OUTPUT / "histograms_by_region.png", dpi=160); plt.close(fig)

    fig, axis = plt.subplots(figsize=(9, 6), constrained_layout=True)
    axis.boxplot(values, tick_labels=names)
    axis.set_title("Promedio de ventas por pedido por región")
    axis.set_xlabel("Región"); axis.set_ylabel("Promedio de ventas por pedido")
    fig.savefig(OUTPUT / "boxplot_by_region.png", dpi=160); plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    for axis, name, value in zip(axes.flat, names, values):
        stats.probplot(value, dist="norm", plot=axis)
        axis.set_title(f"Q–Q: {name}")
    fig.suptitle("Gráficos Q–Q por región")
    fig.savefig(OUTPUT / "qq_plots_by_region.png", dpi=160); plt.close(fig)

    print(f"Clientes analizados: {len(customer_rows)}")
    print(f"H0: {H0}")
    print(f"H1: {H1}")
    print(f"ANOVA clásico: F={classic_stat:.4f}, p={classic_p:.4f}, eta²={eta_squared:.4f}")
    print(f"ANOVA de Welch: F={welch_stat:.4f}, p={welch_p:.4f}")
    print("Decisión: " + ("se rechaza H0; revisar Tukey HSD." if classic_p < ALPHA else "no se rechaza H0; Tukey HSD no es necesario."))
    print(f"Resultados: {OUTPUT}")


if __name__ == "__main__":
    main()
