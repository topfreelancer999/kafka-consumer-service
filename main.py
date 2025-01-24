import os
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer, KafkaProducer
import json
import multiprocessing
import time

from dotenv import load_dotenv
load_dotenv()

# Kafka configuration
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
group_id = os.getenv('KAFKA_GROUP_ID')
topic = os.getenv('KAFKA_TOPIC')
response_topic = os.getenv('KAFKA_RESPONSE_TOPIC')

consumer = KafkaConsumer(
    topic,
    group_id=group_id,
    bootstrap_servers=bootstrap_servers
)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Simulated blocking task
def simulate_blocking_task(message):
    # Simulate a CPU-intensive task
    start_time = time.time()
    while time.time() - start_time < 5:  # Simulate 5 seconds of CPU-bound processing
        pass
    print(f"Processed message: {message}")

# Aggregates hourly data into weekly summaries
def aggregate_hourly_data(hourly_data):
    aggregated_data = {}
    for entry in hourly_data:
        key = f"{entry['Contact Type']}_{entry['Staff Type']}_{entry['Call Center']}"
        if key not in aggregated_data:
            aggregated_data[key] = {
                "Volume": 0,
                "Abandons": 0,
                "Top Line Agents (FTE)": 0,
                "Base AHT": 0,
                "Handled Threshold": 0,
                "Service Level (X Seconds)": 0,
                "Acceptable Wait Time": 0,
                "Total Queue Time": 0
            }        
        aggregated_data[key]["Volume"] += entry["Volume"]
        aggregated_data[key]["Abandons"] += entry["Abandons"]
        aggregated_data[key]["Top Line Agents (FTE)"] += entry["Top Line Agents (FTE)"]
        aggregated_data[key]["Base AHT"] += entry["Base AHT"]
        aggregated_data[key]["Handled Threshold"] += entry["Handled Threshold"]
        aggregated_data[key]["Service Level (X Seconds)"] += entry["Service Level (X Seconds)"]
        aggregated_data[key]["Acceptable Wait Time"] += entry["Acceptable Wait Time"]
        aggregated_data[key]["Total Queue Time"] += entry["Total Queue Time"]
    
    return aggregated_data

# Concurrent message processing
def process_message(message):
    hourly_data = json.loads(message.value.decode('utf-8'))["weekly"]
    aggregated_data = aggregate_hourly_data(hourly_data)
    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        executor.submit(simulate_blocking_task, json.dumps(aggregated_data))

# Main function to consume messages
def consume_messages():
    for message in consumer:
        process_message(message)

if __name__ == "__main__":
    consume_messages()