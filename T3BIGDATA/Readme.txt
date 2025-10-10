Proyecto Big Data – Procesamiento Batch y Streaming con Spark y Kafka

Descripción de la solución:

Este proyecto muestra cómo procesar datos de accidentes de tránsito en Colombia usando **Apache Spark** en dos modalidades:

Batch (batch_app.py): se carga directamente el archivo CSV, se limpian y normalizan las columnas, y se generan estadísticas agregadas (conteo por municipio, departamento y fecha). Los resultados se guardan en formato Parquet para consultas posteriores.  
Streaming (streaming_app.py): se utiliza **Apache Kafka** como sistema de mensajería. El dataset se envía a un tópico de Kafka y Spark Structured Streaming lo consume en tiempo real, mostrando en consola un conteo dinámico de registros por municipio.


Requisitos previos
Ubuntu/Linux  
Java 8+  
Apache Spark 3.4+  
Apache Kafka 3.0+  
ZooKeeper
Python 3.x  


Instrucciones de ejecución

Arrancar Zookeeper
bash
cd /opt/zookeeper-3.4.14/bin
sudo ./zkServer.sh start

2. Arrancar Kafka
bash
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties

3. Crear el tópico
bash
/opt/kafka/bin/kafka-topics.sh --create --topic accidentes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

4. Ejecutar el procesamiento batch
Este script está en batch_app.py:
bash
spark-submit batch_app.py


5. Ejecutar el procesamiento streaming
Este script está en streaming_app.py:
bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  streaming_app.py

6. Enviar datos al tópico
bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic accidentes \
  --bootstrap-server localhost:9092 < datasets/accidentes.csv

7. Visualizar resultados
En la terminal de Spark aparecerán tablas con el conteo de registros por municipio de accidentes