#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import json
import pathlib
from dotenv import load_dotenv

# Import external packages
from kafka import KafkaConsumer

# Import functions from local modules
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "dobler_topic")  # Same topic as producer
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_group_id() -> str:
    """Fetch Kafka consumer group ID from environment or use default."""
    group_id = os.getenv("BUZZ_GROUP_ID", "dobler_group")
    logger.info(f"Consumer group ID: {group_id}")
    return group_id


#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored (can be used to log data)
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Define the log file where the messages will be saved
LOG_FILE: pathlib.Path = DATA_FOLDER.joinpath("dobler_consumer_log.txt")
logger.info(f"Log file: {LOG_FILE}")


#####################################
# Real-Time Analytics Function
#####################################

def analyze_message(message: dict):
    """Example of a real-time analytics function."""
    if "error" in message["message"].lower():
        logger.warning(f"Error message detected: {message}")
    else:
        logger.info(f"Message processed: {message}")


#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for the consumer.
    
    - Creates a Kafka consumer using the `KafkaConsumer` class.
    - Reads messages from the specified Kafka topic.
    - Processes and logs messages.
    """
    
    logger.info("START consumer.")
    
    # Fetch environment variables
    topic = get_kafka_topic()
    group_id = get_group_id()

    # Create the Kafka consumer
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",  # Start reading from the earliest message
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    
    logger.info(f"Consumer connected to topic: {topic}")

    try:
        # Consume messages from the Kafka topic
        for message in consumer:
            logger.info(f"Received message: {message.value}")

            # Real-time analytics or message processing
            analyze_message(message.value)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message consumption: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")
    
    logger.info("END consumer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
