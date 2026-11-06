
# BBIM707 – Part B Task 4
# Step 5: RabbitMQ Producer with Persistent Messages

import csv
import json
import pika
import time
from pathlib import Path

QUEUE = "nyc_taxi_stream"
SOURCE = Path.home() / "BBIM707_Task4" / "taxi_stream_1000.csv"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host="localhost",
        heartbeat=120,
        blocked_connection_timeout=60
    )
)

channel = connection.channel()

# Declare durable quorum queue
channel.queue_declare(
    queue=QUEUE,
    durable=True,
    arguments={"x-queue-type": "quorum"}
)

# Request publisher confirmations
channel.confirm_delivery()

published = 0
failed = 0

print("===== RABBITMQ TAXI PRODUCER =====")

with SOURCE.open(newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        event = {
            "event_id": int(row["event_id"]),
            "trip_distance": float(row["trip_distance"]),
            "fare_amount": float(row["fare_amount"]),
            "trip_duration_minutes": float(
                row["trip_duration_minutes"]
            )
        }

        try:
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE,
                body=json.dumps(event),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    message_id=str(event["event_id"])
                ),
                mandatory=True
            )


            published += 1

            if published % 200 == 0:
                print(
                    f"Confirmed messages: {published}"
                )

        except Exception as error:
            failed += 1
            print(
                f"Failed event {event['event_id']}: {error}"
            )
            break

print("\n===== PRODUCER RESULTS =====")
print("Published Messages:", published)
print("Failed Messages:", failed)
print("Expected Messages:", 1000)
print("Publishing Complete:", published == 1000)

connection.close()
