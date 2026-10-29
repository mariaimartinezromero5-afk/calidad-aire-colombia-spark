import re
import sys
import unicodedata

import matplotlib
matplotlib.use("Agg")  # permite guardar graficos sin pantalla (VM sin entorno grafico)
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ARCHIVO = sys.argv[1] if len(sys.argv) > 1 else "calidad_aire.csv"

spark = SparkSession.builder.appName("CalidadAire_Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def limpiar_nombre(nombre):
    """Quita tildes y espacios: 'Año' -> 'ano', 'Estación' -> 'estacion'."""
    n = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", n.lower()).strip("_")


# 1. CARGA DE DATOS
df = spark.read.csv(ARCHIVO, header=True, inferSchema=True, encoding="UTF-8")
df = df.toDF(*[limpiar_nombre(c) for c in df.columns])

print("\n===== 1. CARGA =====")
df.printSchema()
print("Filas:", df.count(), "| Columnas:", len(df.columns))
df.show(5, truncate=False)

# 2. LIMPIEZA Y TRANSFORMACION
print("\n===== 2. LIMPIEZA =====")
total_inicial = df.count()
df = df.dropDuplicates()
df = df.dropna(subset=["variable", "promedio", "ano"])
# 'promedio' y 'ano' llegan como texto: se normalizan (coma decimal -> punto)
df = df.withColumn(
    "promedio",
    F.regexp_replace(F.col("promedio").cast("string"), ",", ".").cast("double"),
)
# Año: se dejan solo los digitos y se toman los 4 primeros (2.019 / 2019.0 -> 2019)
df = df.withColumn(
    "ano",
    F.substring(F.regexp_replace(F.col("ano").cast("string"), "[^0-9]", ""), 1, 4).cast("int"),
)
df = df.filter(F.col("promedio") >= 0)

# Esta columna viene como texto en el portal: se convierte a numero
if "excedencias_limite_actual" in df.columns:
    df = df.withColumn("excedencias", F.col("excedencias_limite_actual").cast("double"))
else:
    df = df.withColumn("excedencias", F.lit(None).cast("double"))

total_final = df.count()
print("Filas antes:", total_inicial, "| despues de limpiar:", total_final)

print("Valores nulos por columna:")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show(
    truncate=False
)

# 3. ANALISIS EXPLORATORIO (EDA)
print("\n===== 3. EDA =====")
contaminantes = ["PM10", "PM2.5", "O3", "NO2", "SO2", "CO"]
cont = df.filter(F.col("variable").isin(contaminantes))

print("3.1 Registros por variable")
df.groupBy("variable").count().orderBy(F.desc("count")).show(30, truncate=False)

print("3.2 Estaciones distintas con medicion por anio")
estaciones_anio = (
    df.groupBy("ano")
    .agg(F.countDistinct("id_estacion").alias("estaciones"))
    .orderBy("ano")
)
estaciones_anio.show(50)

print("3.3 Estadisticas descriptivas del promedio")
df.select("promedio").describe().show()

print("3.4 Top 10 autoridades ambientales por PM2.5 promedio")
pm25_aut = (
    cont.filter(F.col("variable") == "PM2.5")
    .groupBy("autoridad_ambiental")
    .agg(F.round(F.avg("promedio"), 2).alias("promedio_pm25"))
    .orderBy(F.desc("promedio_pm25"))
    .limit(10)
)
pm25_aut.show(truncate=False)

print("3.5 Evolucion anual de PM10 y PM2.5")
evol = (
    cont.filter(F.col("variable").isin("PM10", "PM2.5"))
    .groupBy("ano", "variable")
    .agg(F.round(F.avg("promedio"), 2).alias("promedio"))
    .orderBy("ano")
)
evol.show(60)

print("3.6 Promedio por contaminante")
por_cont = (
    cont.groupBy("variable")
    .agg(F.round(F.avg("promedio"), 2).alias("promedio"))
    .orderBy(F.desc("promedio"))
)
por_cont.show()

# 4. VISUALIZACION (se guardan como imagenes PNG)
print("\n===== 4. VISUALIZACION =====")

p = pm25_aut.toPandas()
plt.figure(figsize=(10, 5))
plt.barh(p["autoridad_ambiental"], p["promedio_pm25"], color="#1f77b4")
plt.gca().invert_yaxis()
plt.title("Top 10 autoridades ambientales por PM2.5 promedio")
plt.xlabel("ug/m3")
plt.tight_layout()
plt.savefig("grafico_pm25_autoridades.png", dpi=120)
plt.close()

e = evol.toPandas()
plt.figure(figsize=(9, 5))
for var, g in e.groupby("variable"):
    plt.plot(g["ano"], g["promedio"], marker="o", label=var)
plt.title("Evolucion anual del promedio de PM10 y PM2.5")
plt.xlabel("Año")
plt.ylabel("ug/m3")
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig("grafico_evolucion_pm.png", dpi=120)
plt.close()

c = por_cont.toPandas()
plt.figure(figsize=(8, 5))
plt.bar(c["variable"], c["promedio"], color="#2ca02c")
plt.title("Promedio por contaminante")
plt.ylabel("Promedio de las mediciones")
plt.tight_layout()
plt.savefig("grafico_promedio_contaminante.png", dpi=120)
plt.close()

print("Graficos guardados:")
print(" - grafico_pm25_autoridades.png")
print(" - grafico_evolucion_pm.png")
print(" - grafico_promedio_contaminante.png")

spark.stop()
