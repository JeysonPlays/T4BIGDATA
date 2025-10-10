from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("AccidentesStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Leer datos desde Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "accidentes") \
    .load()

# Convertir valor a string
lines = df.selectExpr("CAST(value AS STRING)")

# Separar columnas (asumiendo CSV con comas)
cols = split(lines["value"], ",")
stream_df = lines.select(
    cols.getItem(0).alias("FECHA_HECHO"),
    cols.getItem(1).alias("COD_DEPTO"),
    cols.getItem(2).alias("DEPARTAMENTO"),
    cols.getItem(3).alias("COD_MUNICIPIO"),
    cols.getItem(4).alias("MUNICIPIO"),
    cols.getItem(5).alias("CANTIDAD")
)

# Conteo por municipio
conteo = stream_df.groupBy("MUNICIPIO").count()

# Mostrar resultados en consola
query = conteo.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()