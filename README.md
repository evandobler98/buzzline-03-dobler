# Kafka Producers & Consumers

## Overview
This project includes Kafka producers and consumers for processing and analyzing streaming data. It consists of:
- **JSON Producer:** Streams JSON-formatted messages.
- **CSV Producer:** Reads CSV data, converts it into JSON, and streams it to a Kafka topic.
- **JSON Consumer:** Listens for JSON messages and processes them accordingly.
- **CSV Consumer:** Consumes messages from a Kafka topic and analyzes temperature trends.

## Setup Instructions

### 1️⃣ Install Dependencies
Ensure you have Python and the required dependencies installed:
```sh
pip install -r requirements.txt
```

### 2️⃣ Start Kafka and Zookeeper
Start the necessary Kafka services:
```sh
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### 3️⃣ Run Producers
To start the producers, use:
```sh
python producers/json_producer_dobler.py
python producers/csv_producer_dobler.py
```

### 4️⃣ Run Consumers
To start the consumers, use:
```sh
python consumers/json_consumer_dobler.py
python consumers/csv_consumer_dobler.py
```

## Example Messages
**Example JSON Message:**
```json
{
    "timestamp": "2025-02-04T15:30:00Z",
    "temperature": 225.5
}
```

## Troubleshooting
- **Kafka not running?** Ensure Zookeeper and Kafka services are started.
- **Missing dependencies?** Run `pip install -r requirements.txt`.
- **Consumer not receiving messages?** Check if the producer is running and verify the topic name.

## Logging & Debugging
Logs are stored in `logs/` and can be checked for troubleshooting errors.
```sh
tail -f logs/kafka.log
```


---

This update documents the new Kafka producers and consumers, making it easy for anyone to understand and run them.

