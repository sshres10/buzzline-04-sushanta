"""
project_consumer_sushanta.py

Consume JSON messages from a Kafka topic and visualize average sentiment score by category in real-time.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # Handle JSON parsing
from collections import defaultdict  # Data structure for tracking category sentiments

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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
    topic = os.getenv("PROJECT_TOPIC", "buzzline-topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group ID from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group ID: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Dictionaries to track total sentiment and count per category
category_sentiments = defaultdict(float)  # Tracks total sentiment score per category
category_counts = defaultdict(int)        # Tracks count of messages per category

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots()
plt.ion()  # Enable interactive mode

#####################################
# Define update chart function
#####################################


def update_chart():
    """Update the live chart with the latest average sentiment score by category."""
    # Clear the previous chart
    ax.clear()

    # Calculate average sentiment scores
    categories = list(category_sentiments.keys())
    averages = [
        category_sentiments[cat] / category_counts[cat] if category_counts[cat] > 0 else 0
        for cat in categories
    ]

    # Create a bar chart using the bar() method
    ax.bar(categories, averages, color="skyblue")

    # Set chart labels and title
    ax.set_xlabel("Categories")
    ax.set_ylabel("Average Sentiment Score")
    ax.set_title("Real-Time Average Sentiment Score by Category")

    # Rotate x-axis labels for readability
    ax.set_xticklabels(categories, rotation=45, ha="right")

    # Automatically adjust padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow time for the chart to render
    plt.pause(0.01)


#####################################
# Function to process a single message
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Process the JSON message
        if isinstance(message_dict, dict):
            # Extract the 'category' and 'sentiment' fields from the message
            category = message_dict.get("category", "unknown")
            sentiment = message_dict.get("sentiment", 0.0)

            # Update total sentiment and count for the category
            category_sentiments[category] += sentiment
            category_counts[category] += 1

            # Log the updated values
            logger.info(f"Updated category sentiments: {dict(category_sentiments)}")
            logger.info(f"Updated category counts: {dict(category_counts)}")

            # Update the chart
            update_chart()

            # Log the updated chart
            logger.info(f"Chart updated successfully for message: {message}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # Extract the message value as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Execute
#####################################

if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
