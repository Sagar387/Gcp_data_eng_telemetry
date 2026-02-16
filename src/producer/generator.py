import time
import json
import random
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer

# 1. Load Environment Variables
load_dotenv()

# 2. Configuration
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
    'client.id': 'python-producer-01'
}

TOPIC_NAME = 'athlete_telemetry'

# --- THE SIMULATION ENGINE ---
class AthleteSimulation:
    def __init__(self, athlete_id):
        self.athlete_id = athlete_id
        self.heart_rate = random.randint(70, 90)
        self.speed = 0
        # Start at Toronto Waterfront
        self.latitude = 43.6426 
        self.longitude = -79.3871
        
    def next_state(self):
        """
        Generates the next second of data.
        Includes INTENTIONAL ERRORS for Data Engineering practice.
        """
        
        # 1. Update Speed (Walk/Run/Sprint cycles)
        # Randomly accelerate or decelerate
        speed_change = random.uniform(-0.5, 0.5)
        self.speed = max(0, min(15, self.speed + speed_change)) # Clamp 0-15 km/h

        # 2. Update Location (Move ~5-10 meters/sec)
        # 0.0001 deg is roughly 11 meters
        self.latitude += 0.00001 * self.speed 
        self.longitude += 0.00001 * random.uniform(-0.5, 0.5)

        # 3. CALCULATE HEART RATE (Physiologically accurate)
        target_hr = 60 + (self.speed * 10) 
        # Smoothly move current HR towards target HR (Exponential Moving Average)
        self.heart_rate += (target_hr - self.heart_rate) * 0.1 
        # Add a little natural noise (jitter)
        noise = random.uniform(-2, 2)
        
        # --- THE CHAOS SECTION (Data Engineering Challenges) ---
        
        final_hr = self.heart_rate + noise
        
        # Chaos 1: The "Null" Value (1% chance)
        # Scenario: Sensor momentarily disconnects from skin.
        if random.random() < 0.01:
            final_hr = None 
            
        # Chaos 2: The "Outlier" (0.5% chance)
        # Scenario: Electrical interference causes a massive spike.
        elif random.random() < 0.005:
            final_hr = random.randint(300, 400) # Impossible human HR

        # Create the standard clean event
        event = {
            "event_uuid": str(uuid.uuid4()),
            "athlete_id": self.athlete_id,
            "timestamp": datetime.now().isoformat(),
            "heart_rate": round(final_hr) if final_hr else None,
            "speed": round(self.speed, 2),
            "location": f"{self.latitude:.6f},{self.longitude:.6f}"
        }

        # Return a LIST of events (usually just 1, sometimes 2 for duplicates)
        # Chaos 3 Logic: Duplicate Data (1% chance)
        # Scenario: Network retry sends the same packet twice.
        if random.random() < 0.01:
            return [event, event] 
        
        return [event]

if __name__ == '__main__':
    print("🚀 Realistic PulseStream Producer Starting...")
    
    # Check config
    if not conf['bootstrap.servers']:
        print("❌ ERROR: Missing config. Did you set up the .env file?")
        exit(1)

    producer = Producer(conf)

    # Create simulation objects for 3 athletes
    athletes = [AthleteSimulation(f"athlete_{i}") for i in range(1, 4)]

    try:
        while True:
            for athlete in athletes:
                # Get the next batch of events (usually 1, sometimes 2)
                events = athlete.next_state()
                
                for event in events:
                    # Send to Kafka
                    producer.produce(
                        TOPIC_NAME,
                        key=event['athlete_id'],
                        value=json.dumps(event)
                    )
                    
                    # Console Logging
                    hr_status = f"{event['heart_rate']} bpm" if event['heart_rate'] else "⚠️ NULL"
                    if event['heart_rate'] and event['heart_rate'] > 250:
                        hr_status = f"🔥 {event['heart_rate']} bpm (OUTLIER)"
                        
                    print(f"🏃 {event['athlete_id']} | Speed: {event['speed']} km/h | HR: {hr_status}")

            # Flush ensures the messages are actually sent
            producer.poll(0)
            
            # Wait 1 second before next tick
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
        producer.flush()