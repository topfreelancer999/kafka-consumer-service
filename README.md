# Kafka Consumer Service with Data Aggregation

# Installation
## Clone the repository:

git clone https://github.com/topfreelancer999/kafka-consumer-service.git

## Install dependencies:

pip install -r requirements.txt

## Create a .env file and fill in the required Kafka configuration variables:

KAFKA_BOOTSTRAP_SERVERS=localhost:9092

KAFKA_GROUP_ID=my-consumer-group

KAFKA_TOPIC=Scenario-Execute

KAFKA_RESPONSE_TOPIC=Scenario-Execute-Response

# Running the Service

Run the Kafka consumer service with the following command:

python main.py

# Concurrency Approach

The service uses a ThreadPoolExecutor to achieve concurrency. The number of worker threads is set to the number of CPU cores on the host machine using multiprocessing.cpu_count(). This allows for concurrent processing of messages without blocking subsequent messages.

# Limitations

- The simulated blocking task in the service is CPU-bound, which can be impacted by Python’s Global Interpreter Lock (GIL) when using threads. While the GIL limits true parallelism in CPU-bound tasks, the use of threads in this scenario allows for efficient I/O-bound concurrency.
- To scale further in a production environment with CPU-bound tasks, consider using a ProcessPoolExecutor for true parallelism, bypassing the GIL limitations. However, this approach introduces additional overhead due to inter-process communication.
