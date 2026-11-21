"""Educational single-worker relay; not production-safe for concurrent workers."""
import json
import os
import psycopg
from confluent_kafka import Producer

TOPIC = "mbr.order.events"
producer = Producer({"bootstrap.servers": "localhost:29092", "acks": "all", "enable.idempotence": True})

def publish(event):
    event_id, aggregate_id, event_type, payload = event
    outcomes = []
    def delivered(err, msg):
        outcomes.append((err, msg.partition() if err is None else None, msg.offset() if err is None else None))
    producer.produce(TOPIC, key=str(aggregate_id),
                     value=json.dumps({"event_id":str(event_id),"event_type":event_type,"payload":payload}).encode(),
                     on_delivery=delivered)
    remaining = producer.flush(15)
    if remaining or len(outcomes) != 1 or outcomes[0][0] is not None:
        raise RuntimeError(f"Kafka ACK not confirmed: {outcomes}")
    print(f"Kafka acknowledged event {event_id}, partition={outcomes[0][1]}, offset={outcomes[0][2]}", flush=True)

with psycopg.connect(host="127.0.0.1", port=5432, dbname="mbr_outbox_lab",
                     user="shopflow_relay", password=os.environ["PGPASSWORD"]) as conn:
    events = conn.execute("SELECT event_id,aggregate_id,event_type,payload FROM outbox_events WHERE published_at IS NULL ORDER BY created_at,event_id").fetchall()
    print(f"Found {len(events)} pending events", flush=True)
    for event in events:
        publish(event)
        if os.getenv("CRASH_AFTER_KAFKA_ACK") == "1":
            print("SIMULATED CRASH: Kafka ACK received; PostgreSQL not updated", flush=True)
            os._exit(42)
        conn.execute("UPDATE outbox_events SET published_at=now() WHERE event_id=%s AND published_at IS NULL", (event[0],))
        conn.commit()
        print(f"Marked event {event[0]} as published", flush=True)
