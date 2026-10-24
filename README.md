# Tarea 3 - Procesamiento de Datos con Apache Spark - Películas de Disney

Repositorio para la entrega del proyecto de el curso Big Data utilizando Apache Kafka, Apache Spark Streaming y un dataset de películas de Disney `disney_movies.csv`. El objetivo del sistema es procesar un flujo continuo de datos para generar métricas limpias y ordenadas por género cinematográfico.



## Tecnologías Utilizadas
* Python 3. (Lenguaje de programación principal)
* Apache Kafka (Productor y canal de mensajería en tiempo real)
* Apache Spark Streaming (Procesamiento distribuido de datos en streaming)
* GitHub (Control de versiones y almacenamiento remoto)


## Estructura del Proyecto
El repositorio contiene los scripts principales del flujo:
* `kafka_producer.py`: Este Lee el archivo CSV de películas de Disney y transmite los registros al *topic* de Kafka en tiempo real.
* `spark_streaming_consumer.py`: Consume los eventos desde Kafka, realiza la limpieza de datos erróneos y calcula las agregaciones financieras agrupadas por género.



## Resultados y Métricas Analíticas
El sistema procesa y genera dinámicamente las siguientes métricas clave para la toma de decisiones:
1. Conteo total de películas por género.
2. Recaudación total acumulada
3. Promedio de ingresos.
4. Película más taquillera identificada por cada género.



## Instrucciones de Ejecución

1. Iniciar el servicio de Zookeeper y Kafka en el entorno local o máquina virtual.
2. Crear el topic de Kafka correspondiente:
   ```bash
   kafka-topics.sh --create --topic disney_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
3. Ejecutar para enviar los datos
   
 python3 kafka_producer.py
 
4.Ejecutar el Consumidor de Spark para procesar el flujo y visualizar la tabla analítica

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.x.x spark_streaming_consumer.py
