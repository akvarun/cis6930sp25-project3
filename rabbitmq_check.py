import pika
import json
from loguru import logger

# Configure logging
logger.add("rabbitmq_check.log", rotation="1 MB", retention="3 days", 
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

def check_rabbitmq(host: str, exchange: str = 'ufo'):
    """
    Basic script to check RabbitMQ connection and log incoming messages
    """
    try:
        # Establish connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                credentials=pika.PlainCredentials('guest', 'guest')
            )
        )
        channel = connection.channel()
        
        # Declare exchange (same as publisher)
        channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        
        # Create temporary queue
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        
        # Bind to exchange
        channel.queue_bind(exchange=exchange, queue=queue_name)
        
        logger.info(f"Successfully connected to RabbitMQ server at {host}")
        logger.info(f"Listening on exchange '{exchange}' (fanout)")
        logger.info("Waiting for messages. Press Ctrl+C to exit...")

        def callback(ch, method, properties, body):
            try:
                message = json.loads(body.decode())
                logger.info(f"Received message: {message}")
            except json.JSONDecodeError:
                logger.warning(f"Received non-JSON message: {body.decode()}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

        # Start consuming
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True
        )

        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError:
        logger.error(f"Failed to connect to RabbitMQ server at {host}")
    except KeyboardInterrupt:
        logger.info("Gracefully shutting down...")
        connection.close()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if 'connection' in locals() and connection.is_open:
            connection.close()

if __name__ == "__main__":
    # Replace with your actual RabbitMQ host
    RABBITMQ_HOST = "cpu001.cm.cluster"  
    
    logger.info("Starting RabbitMQ connection check...")
    check_rabbitmq(RABBITMQ_HOST)
