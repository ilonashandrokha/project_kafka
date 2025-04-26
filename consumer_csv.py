import json
import csv
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'zamowienia_elektronika',
    bootstrap_servers='localhost:9092',
    group_id='consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


with open('orders.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['id', 'timestamp', 'nazwa_produktu', 'marka', 'kategoria_produktu', 'ilosc', 'cena'])
    writer.writeheader() 


    for message in consumer:
        order = message.value
        writer.writerow(order)
        print(f'Zapisano zamówienie: {order}')
