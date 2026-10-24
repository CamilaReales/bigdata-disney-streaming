from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic_name = 'disney_topic'

print("=== ENVIANDO DATOS COMPLETOS DE DISNEY ===")

with open('disney_movies.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Enviamos: título, fecha, género, clasificación (mpaa), recaudación normal y ajustada
        mensaje = f"{row['movie_title']},{row['release_date']},{row['genre']},{row['mpaa_rating']},{row['total_gross']},{row['inflation_adjusted_gross']}"
        producer.send(topic_name, value=mensaje.encode('utf-8'))
        print(f"Enviado: {row['movie_title']}")  
