import pika
import socket
from loguru import logger
import concurrent.futures
import time

# Configure logging
logger.add("rabbitmq_scan.log", rotation="1 MB", retention="1 day",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

def test_rabbitmq_connection(host):
    """Test connection to a specific RabbitMQ host"""
    hostname = f"{host}.cm.cluster"
    result = {"host": hostname, "success": False, "error": None}
    
    try:
        # First check basic connectivity
        socket.create_connection((hostname, 5672), timeout=5)
        
        # Then test RabbitMQ connection
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(
            host=hostname,
            port=5672,
            credentials=credentials,
            connection_attempts=2,
            retry_delay=1,
            socket_timeout=3
        )
        
        connection = pika.BlockingConnection(parameters)
        connection.close()
        result["success"] = True
        logger.success(f"✅ Successfully connected to {hostname}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.warning(f"❌ Failed to connect to {hostname}: {str(e)}")
    
    return result

def scan_all_servers():
    """Scan all possible servers in parallel"""
    hosts_to_scan = []
    
    # Generate all possible hostnames
    hosts_to_scan.extend([f"cpu{num:03d}" for num in range(1, 3)])  # cpu001-cpu002
    hosts_to_scan.extend([f"gpu{num:03d}" for num in range(1, 23)])  # gpu001-gpu022
    
    logger.info(f"Starting scan of {len(hosts_to_scan)} RabbitMQ servers...")
    
    successful_connections = []
    
    # Use ThreadPoolExecutor to scan in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_host = {
            executor.submit(test_rabbitmq_connection, host): host 
            for host in hosts_to_scan
        }
        
        for future in concurrent.futures.as_completed(future_to_host):
            result = future.result()
            if result["success"]:
                successful_connections.append(result["host"])
    
    # Print summary
    logger.info("\n=== Scan Results ===")
    if successful_connections:
        logger.success("Found working RabbitMQ servers:")
        for host in successful_connections:
            logger.success(f"  - {host}")
    else:
        logger.error("No working RabbitMQ servers found")
    
    return successful_connections

if __name__ == "__main__":
    start_time = time.time()
    working_servers = scan_all_servers()
    duration = time.time() - start_time
    
    logger.info(f"\nScan completed in {duration:.2f} seconds")
    
    if working_servers:
        logger.info("\nTo use a working server, update your code with:")
        logger.info(f"host = '{working_servers[0]}'")
