#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
from collections import deque  # Track recent messages

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SENSOR_TOPIC", "dobler_sensor_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SENSOR_CONSUMER_GROUP_ID", "dobler_sensor_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_stability_threshold() -> float:
    """Fetch the allowed sensor variation threshold."""
    threshold = float(os.getenv("SENSOR_STABILITY_THRESHOLD", 0.5))
    logger.info(f"Sensor stability threshold: {threshold}")
    return threshold

def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SENSOR_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

#####################################
# Define a function to detect stability
#####################################

def detect_stability(rolling_window_deque: deque) -> bool:
    """
    Detect if sensor readings are stable over time.

    Args:
        rolling_window_deque (deque): Rolling window of temperature readings.

    Returns:
        bool: True if readings are stable, False otherwise.
    """
    WINDOW_SIZE: int = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        logger.debug(f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE} readings.")
        return False

    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stable: bool = temp_range <= get_stability_threshold()
    logger.debug(f"Sensor range: {temp_range}. Stable: {is_stable}")
    return is_stable

#####################################
# Function to process a single message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON-transferred CSV message and check for stability.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of sensor readings.
        window_size (int): Size of the rolling window.
    """
    try:
        logger.debug(f"Raw message: {message}")
        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        rolling_window.append(temperature)

        if detect_stability(rolling_window):
            logger.info(f"STABLE READINGS DETECTED at {timestamp}: Temp stable at {temperature}°C over last {window_size} readings.")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)
    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
