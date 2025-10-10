from pyspark.sql import SparkSession

# ==============================
# 1. Crear sesión de Spark
# ==============================
spark = SparkSession.builder.appName("AccidentesBatch").getOrCreate()

# ==============================
# 2. Cargar dataset CSV
# ==============================
df = spark.read.csv("/home/vboxuser/datasets/accidentes.csv", header=True, inferSchema=True)

# ==============================
# 3. Normalizar nombres de columnas
#    (quita espacios, tildes y pone mayúsculas)
# ==============================
for col in df.columns:
    new_col = col.strip().upper().replace(" ", "_").replace("Á","A").replace("É","E").replace("Í","I").replace("Ó","O").replace("Ú","U")
    df = df.withColumnRenamed(col, new_col)

# ==============================
# 4. Mostrar esquema y primeras filas
# ==============================
df.printSchema()
df.show(5)

# ==============================
# 5. Limpieza básica
# ==============================
df = df.dropna(subset=["MUNICIPIO", "DEPARTAMENTO"])

# ==============================
# 6. Análisis con API DataFrame
# ==============================
print("=== Conteo por MUNICIPIO ===")
df.groupBy("MUNICIPIO").count().show(10)

print("=== Conteo por DEPARTAMENTO ===")
df.groupBy("DEPARTAMENTO").count().show(10)

print("=== Conteo por FECHA_HECHO ===")
df.groupBy("FECHA_HECHO").count().show(10)

# ==============================
# 7. Análisis con SQL (opcional)
# ==============================
df.createOrReplaceTempView("tabla")

print("=== SQL: Conteo por FECHA_HECHO ===")
spark.sql("""
    SELECT FECHA_HECHO, COUNT(*) AS TOTAL
    FROM tabla
    GROUP BY FECHA_HECHO
    ORDER BY FECHA_HECHO
""").show(10)

# ==============================
# 8. Guardar resultados en formato Parquet
# ==============================
df.write.mode("overwrite").parquet("/home/vboxuser/resultados/accidentes_batch")