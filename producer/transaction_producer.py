import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "transactions"
)

EVENT_INTERVAL_SECONDS = float(
    os.getenv("EVENT_INTERVAL_SECONDS", "1")
)


PRODUCTS = [
    ("PROD-101", "Laptop", "Electronics", 899.99),
    ("PROD-102", "Headphones", "Electronics", 129.99),
    ("PROD-103", "Smartphone", "Electronics", 699.99),
    ("PROD-201", "Running Shoes", "Sports", 89.99),
    ("PROD-202", "Yoga Mat", "Sports", 29.99),
    ("PROD-301", "Coffee Maker", "Home", 79.99),
    ("PROD-302", "Desk Lamp", "Home", 39.99),
    ("PROD-401", "Backpack", "Fashion", 59.99),
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "PayPal",
    "Digital Wallet",
]

COUNTRIES = [
    "United States",
    "Canada",
    "United Kingdom",
    "Germany",
    "India",
]


def create_producer():
    """Create and return a Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )


def generate_transaction():
    """Generate one synthetic e-commerce transaction."""

    product_id, product_name, category, base_price = random.choice(PRODUCTS)

    quantity = random.randint(1, 5)

    # Slight price variation makes the stream more realistic.
    unit_price = round(
        base_price * random.uniform(0.95, 1.05),
        2
    )

    transaction = {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "country": random.choice(COUNTRIES),
    }

    return transaction


def publish_transaction(producer, transaction):
    """Publish a transaction event to Kafka."""

    future = producer.send(
        KAFKA_TOPIC,
        key=transaction["transaction_id"],
        value=transaction,
    )

    try:
        metadata = future.get(timeout=10)

        print(
            f"Published {transaction['transaction_id']} "
            f"to topic={metadata.topic}, "
            f"partition={metadata.partition}, "
            f"offset={metadata.offset}"
        )

    except KafkaError as error:
        print(f"Failed to publish transaction: {error}")
        raise


def main():
    """Continuously generate and publish transaction events."""

    print("Starting real-time transaction producer...")
    print(f"Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    producer = create_producer()

    try:
        while True:
            transaction = generate_transaction()

            publish_transaction(
                producer,
                transaction
            )

            print(
                json.dumps(
                    transaction,
                    indent=2
                )
            )

            time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping transaction producer...")

    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    main()
