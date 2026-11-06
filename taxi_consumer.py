
# BBIM707 – Part B Task 4
# RabbitMQ Consumer: Validation, Storage and Acknowledgements

import json
import sqlite3
import pika
from pathlib import Path

QUEUE = "nyc_taxi_stream"
DB_PATH = Path.home() / "BBIM707_Task4" / "taxi_stream.db"

# Create local analytical storage
db = sqlite3.connect(DB_PATH)

db.execute("""
CREATE TABLE IF NOT EXISTS taxi_events (
    event_id INTEGER PRIMARY KEY,
    trip_distance REAL NOT NULL,
    fare_amount REAL NOT NULL,
    trip_duration_minutes REAL NOT NULL
)
""")
db.commit()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host="localhost",
        heartbeat=120
    )
)

channel = connection.channel()

channel.queue_declare(
    queue=QUEUE,
    durable=True,
    arguments={"x-queue-type": "quorum"}
)

# Process one unacknowledged message at a time
channel.basic_qos(prefetch_count=1)

processed = 0
duplicates = 0
invalid = 0

print("===== RABBITMQ TAXI CONSUMER =====")

def process_message(ch, method, properties, body):
    global processed, duplicates, invalid

    try:
        event = json.loads(body)

        event_id = int(event["event_id"])
        distance = float(event["trip_distance"])
        fare = float(event["fare_amount"])
        duration = float(event["trip_duration_minutes"])

        # Validate required measurements
        if (
            event_id <= 0
            or distance <= 0
            or fare < 0
            or duration <= 0
            or duration > 240
        ):
            raise ValueError("Invalid taxi record")

        # Prevent duplicate records using event_id
        cursor = db.execute("""
            INSERT OR IGNORE INTO taxi_events
            (event_id, trip_distance, fare_amount,
             trip_duration_minutes)
            VALUES (?, ?, ?, ?)
        """, (event_id, distance, fare, duration))

        db.commit()

        if cursor.rowcount == 1:
            processed += 1
        else:
            duplicates += 1

        # Acknowledge only after successful database commit
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if (processed + duplicates) % 200 == 0:
            print(
                f"Handled: {processed + duplicates}, "
                f"Inserted: {processed}, "
                f"Duplicates: {duplicates}"
            )

    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        invalid += 1
        print("Invalid message:", error)

        # Reject invalid data without requeueing
        ch.basic_reject(
            delivery_tag=method.delivery_tag,
            requeue=False
        )

    except Exception as error:
        print("Processing error:", error)

        # Requeue messages that fail unexpectedly
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )
        raise

channel.basic_consume(
    queue=QUEUE,
    on_message_callback=process_message,
    auto_ack=False
)

print("Waiting for taxi messages...")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
finally:
    if connection.is_open:
        connection.close()
    db.close()

    print("\n===== CONSUMER RESULTS =====")
    print("New Records Inserted:", processed)
    print("Duplicate Events Skipped:", duplicates)
    print("Invalid Messages:", invalid)
