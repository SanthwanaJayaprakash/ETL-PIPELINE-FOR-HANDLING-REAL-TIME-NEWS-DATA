from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import requests
import time
from datetime import datetime

# Define Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Keywords for news API
keywords = ['bitcoin', 'war', 'election', 'monsoon', 'health']

def fetch_and_send_news():
    try:
        for keyword in keywords:
            # Fetch news articles from API
            response = requests.get(f'https://newsapi.org/v2/everything?q={keyword}&apiKey=bf5e7a25667248d2858669b2fa5ebe6f')
            data = response.json()
            
            for article in data.get('articles', []):
                news = {
                    'title': article.get('title'),
                    'author': article.get('author'),
                    'description': article.get('description'),
                    'timestamp': datetime.now().isoformat()  # Add timestamp
                }
                
                # Produce a message to Kafka topic 'test'
                future = producer.send('test1', value=news)
                time.sleep(10)
                
                try:
                    record_metadata = future.get(timeout=10)
                    print(f"Message sent successfully to topic {record_metadata.topic} at partition {record_metadata.partition} offset {record_metadata.offset}")
                    print(f"Timestamp: {news['timestamp']}")  # Print timestamp
                except KafkaError as e:
                    print(f"Error sending message: {e}")
    except Exception as ex:
        print(f"Exception occurred: {ex}")

try:
    while True:
        fetch_and_send_news()
        print("Sleeping for 10 seconds...")
        time.sleep(10)  # Sleep for 10 seconds
except KeyboardInterrupt:
    print("Stopping the producer...")
finally:
    # Close the Kafka producer
    producer.flush()  # Ensure all messages are delivered
    producer.close()
    print("Producer closed.")