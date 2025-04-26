import pandas as pd
import random
import time
import json
from datetime import datetime
from kafka import KafkaProducer

# Marki elektroniki
brands = ['Apple', 'Samsung', 'Sony', 'LG', 'Dell', 'HP', 'Lenovo', 'Asus', 'Xiaomi', 'JBL', 'Bose', 'Anker']

# Produkty i kategorie
products_by_category = {
    'Laptopy': ['Laptop gamingowy', 'Laptop ultrabook', 'Laptop biznesowy', 'Laptop 2w1'],
    'Smartfony': ['Smartfon premium', 'Smartfon budżetowy', 'Smartfon ze średniej półki', 'Smartfon dla graczy'],
    'Audio': ['Słuchawki bezprzewodowe', 'Głośnik Bluetooth', 'Soundbar', 'Zestaw kina domowego'],
    'Telewizory': ['Telewizor OLED', 'Telewizor QLED', 'Telewizor LED', 'Telewizor 4K UHD'],
    'Akcesoria': ['Ładowarka', 'Powerbank', 'Etui na telefon', 'Kabel USB-C', 'Hub USB']
}

# Zakresy cenowe dla kategorii
price_ranges = {
    'Laptopy': (2000, 8000),
    'Smartfony': (1000, 6000),
    'Audio': (100, 3000),
    'Telewizory': (1500, 10000),
    'Akcesoria': (30, 500)
}

# Przygotowanie listy par (produkt, kategoria)
product_category_pairs = [(product, category) for category, products in products_by_category.items() for product in products]

quantities = list(range(1, 5))

# Konfiguracja Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

order_id = 1

while True:
    # Losowanie produktu i kategorii
    product, category = random.choice(product_category_pairs)
    
    # Losowanie ceny w zależności od kategorii
    min_price, max_price = price_ranges[category]
    price = round(random.uniform(min_price, max_price), 2)
    
    # Tworzenie zamówienia
    order = {
        'id': order_id,
        'timestamp': datetime.now().isoformat(),
        'nazwa_produktu': product,
        'marka': random.choice(brands),
        'kategoria_produktu': category,
        'ilosc': random.choice(quantities),
        'cena': price
    }
    
    # Wypisanie zamówienia na konsolę
    print(f'Wysyłam zamówienie: {order}')
    
    # Wysłanie do Kafka (topic: zamowienia_elektronika)
    producer.send('zamowienia_elektronika', value=order)
    
    # Przygotowanie następnego ID
    order_id += 1
    
    # Mała przerwa (1-5 sekund)
    time.sleep(random.randint(1, 5))
