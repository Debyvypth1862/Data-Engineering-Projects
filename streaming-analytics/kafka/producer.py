from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime

def create_clickstream_event():
    users = [f"user_{i}" for i in range(1000)]  # Simulate 1000 different users
    urls = ["/home", "/search", "/product", "/cart", "/checkout", "/category", "/profile", "/settings"]
    actions = ["click", "view", "scroll", "hover"]
    
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "url": random.choice(urls),
        "action": random.choice(actions),
        "timestamp": datetime.now().isoformat(),
        "session_id": str(uuid.uuid4())[:8],
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "browser": random.choice(["chrome", "firefox", "safari", "edge"])
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    events_generated = 0
    batch_size = 1000  # Number of events to generate per batch
    total_events = 10_000_000  # Total number of events to generate

    print("Starting clickstream data generation...")
    
    try:
        while events_generated < total_events:
            batch_events = [create_clickstream_event() for _ in range(batch_size)]
            
            for event in batch_events:
                producer.send('clickstream', event)
            
            events_generated += batch_size
            if events_generated % 100000 == 0:
                print(f"Generated {events_generated:,} events")
            
            # Small sleep to prevent overwhelming the system
            time.sleep(0.01)
            
        producer.flush()
        print(f"Completed generating {total_events:,} events!")
        
    except KeyboardInterrupt:
        print("\nStopping data generation...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()