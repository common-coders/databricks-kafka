import subprocess
import time


# Function for start the zookeeper server
def start_zookeeper(zookeeper_path):
    try:
        # Command to start the zookeeper
        zookeeper_start_cmd = f"{zookeeper_path}\\bin\windows\zookeeper-server-start.bat {zookeeper_path}\config\zookeeper.properties"

        # Start Zookeeper
        zookeeper_process = subprocess.Popen(zookeeper_start_cmd, shell= True)

        print('Starting Zookeeper...')
        time.sleep(5)
        print("Zookeeper started successfully.")

        return zookeeper_process
    except Exception as e:
        print("Failed to start Zookeeper:", e)
        return None
    

# Function for start the Kafka server 
def start_kafka(kafka_path):
    try:
        # Command to start the Kafka server
        kafka_start_cmd = f'{kafka_path}\\bin\windows\kafka-server-start.bat {kafka_path}\config\server.properties'
         
        # start the Kafka server
        kafka_process = subprocess.Popen(kafka_start_cmd, shell=True)
        print("Starting Kafka...")
        time.sleep(5)
        print("Kafka started successfully.")

        return kafka_process
    except Exception as e:
        print("Failed to start the Kafka server:", e)
        return None

# Funcion to stop the zookeeper 
def stop_zookeeper(kafka_path):
    try:
        # command to stop the zookeeper server 
        zookeeper_stop_cmd = f'{kafka_path}\\bin\windows\zookeeper-server-stop.bat'
        subprocess.Popen(zookeeper_stop_cmd, shell=True)
        print("The zookeeper server is stoped successfully.")
    except Exception as e:
        print(f"Failed to stop zookeeper:", e)


# Function to stop the Kafka
def stop_kafka(kafka_path):
    try:
        # command to stop the kafka server
        kafka_stop_cmd =  f'{kafka_path}\\bin\windows\kafka-server-stop.bat'
        subprocess.Popen(kafka_stop_cmd, shell=True)
        print('Kafka server is stoped successfully.')
    except Exception as e:
        print(f'Failed to stop Kafka:', e)

   # import subprocess

class ZookeeperManager:
    def __init__(self, zookeeper_path, config_path):
        self.zookeeper_path = zookeeper_path
        self.config_path = config_path

    def start_in_terminal(self):
        """Starts Zookeeper in a new terminal window."""
        print("Starting Zookeeper in a new terminal...")
        subprocess.Popen(
            ["start", "cmd", "/k", self.zookeeper_path, self.config_path],
            shell=True
        )

    def start_with_output(self):
        """Starts Zookeeper and captures output in the current terminal window."""
        print("Starting Zookeeper with real-time output...")
        zookeeper_process = subprocess.Popen(
            [self.zookeeper_path, self.config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        for line in iter(zookeeper_process.stdout.readline, ''):
            print("Zookeeper:", line, end='')


class KafkaManager:
    def __init__(self, kafka_path, config_path):
        self.kafka_path = kafka_path
        self.config_path = config_path

    def start_in_terminal(self):
        """Starts Kafka in a new terminal window."""
        print("Starting Kafka in a new terminal...")
        subprocess.Popen(
            ["start", "cmd", "/k", self.kafka_path, self.config_path],
            shell=True
        )

    def start_with_output(self):
        """Starts Kafka and captures output in the current terminal window."""
        print("Starting Kafka with real-time output...")
        kafka_process = subprocess.Popen(
            [self.kafka_path, self.config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        for line in iter(kafka_process.stdout.readline, ''):
            print("Kafka:", line, end='')


# Example usage
if __name__ == "__main__":
    zookeeper_manager = ZookeeperManager(
        zookeeper_path="c:\\kafka\\bin\\windows\\zookeeper-server-start.bat",
        config_path="c:\\kafka\\config\\zookeeper.properties"
    )

    kafka_manager = KafkaManager(
        kafka_path="c:\\kafka\\bin\\windows\\kafka-server-start.bat",
        config_path="c:\\kafka\\config\\server.properties"
    )

    # Choose the preferred method to start Zookeeper and Kafka

    # Method 1: Start in new terminal windows
    # zookeeper_manager.start_in_terminal()
    # kafka_manager.start_in_terminal()

    # Method 2: Start with real-time output in current terminal
    # zookeeper_manager.start_with_output()
    # kafka_manager.start_with_output()


#import subprocess

class ZookeeperService:
    def __init__(self, zookeeper_path, config_path):
        self.zookeeper_path = zookeeper_path
        self.config_path = config_path

    def start_in_console(self):
        """Starts Zookeeper and logs output in the same console."""
        print("Starting Zookeeper in current console...")
        process = subprocess.Popen(
            [self.zookeeper_path, self.config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        for line in iter(process.stdout.readline, ''):
            print("Zookeeper:", line, end='')  # Print each line as it's available

    def start_in_new_terminal(self):
        """Starts Zookeeper in a new terminal window."""
        print("Starting Zookeeper in a new terminal...")
        subprocess.Popen(
            ["start", "cmd", "/k", self.zookeeper_path, self.config_path],
            shell=True
        )


class KafkaService:
    def __init__(self, kafka_path, config_path):
        self.kafka_path = kafka_path
        self.config_path = config_path

    def start_in_console(self):
        """Starts Kafka and logs output in the same console."""
        print("Starting Kafka in current console...")
        process = subprocess.Popen(
            [self.kafka_path, self.config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        for line in iter(process.stdout.readline, ''):
            print("Kafka:", line, end='')  # Print each line as it's available

    def start_in_new_terminal(self):
        """Starts Kafka in a new terminal window."""
        print("Starting Kafka in a new terminal...")
        subprocess.Popen(
            ["start", "cmd", "/k", self.kafka_path, self.config_path],
            shell=True
        )


# Example usage:
if __name__ == "__main__":
    # Paths to the Zookeeper and Kafka scripts and configuration files
    zookeeper_path = "c:\\kafka\\bin\\windows\\zookeeper-server-start.bat"
    zookeeper_config = "c:\\kafka\\config\\zookeeper.properties"
    kafka_path = "c:\\kafka\\bin\\windows\\kafka-server-start.bat"
    kafka_config = "c:\\kafka\\config\\server.properties"

    # Initialize services
    zookeeper_service = ZookeeperService(zookeeper_path, zookeeper_config)
    kafka_service = KafkaService(kafka_path, kafka_config)

    # Start Zookeeper and Kafka
    # Option 1: Start in the same console
    # zookeeper_service.start_in_console()
    # kafka_service.start_in_console()

    # Option 2: Start in new terminal windows
    zookeeper_service.start_in_new_terminal()
    kafka_service.start_in_new_terminal()
