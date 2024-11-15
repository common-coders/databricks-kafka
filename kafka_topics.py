from confluent_kafka.admin import AdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import time
import json

# List the topics in the given server
def list_topics(bootstrap_server = "localhost:9092"):
    admin_client = AdminClient({"bootstrap.servers":bootstrap_server})
    topics = admin_client.list_topics().topics
    #print(topics)
    print("Topics in Kafka are :")
    for topic, metadata in topics.items():
        print(f"Topic: {topic}, Partitions: {len(metadata.partitions)}")


# Create 
def create_topic(topicname , bootstrap_server = "localhost:9092", num_partitions = 1, replication_factor = 1): # Declare a function with parameter
    admin_client = AdminClient({"bootstrap.servers":bootstrap_server})
    # Create the new topic with given name 
    new_topic = NewTopic(topicname, num_partitions, replication_factor)
    # print (new_topic)
    admin_client.create_topics([new_topic])
    while True:
        metadata = admin_client.list_topics(timeout = 10).topics
        if topicname in metadata:
            print (f'The Topic `{topicname}` was created successfully.')
            break
        time.sleep(1)
    list_topics(bootstrap_server)


# # Create a New topic with creating a admin_client inside the function
# def create_topic_with_server(topicname, bootstrap_server ,  num_partitions = 1, replication_factor = 1): 
#     admin_client = AdminClient({"bootstrap.servers": bootstrap_server})
#     # Create the new topic
#     new_topic = NewTopic(topicname, num_partitions = 1, replication_factor = 1)
#     admin_client.create_topics([new_topic])
#     print (f'Topic {topicname} created successfully.')

# def delete_topic(topicname, bootstrap_server = "localhost:9092"):
#     admin_client = AdminClient({"bootstrap.servers":bootstrap_server})
#     fs = admin_client.delete_topics([topicname], operation_timeout = 30) 
#     while True:
#         metadata = admin_client.list_topics(timeout = 10).topics
#         if topicname in metadata:
#             print (f"The topic `{topicname}` is deleted successfully.")
#             break
#         time.sleep(1)
#     print("Remanining Topics are :")
#     list_topics(bootstrap_server=bootstrap_server)
def send_data_to_topic(topicname, data, bootstrap_server = "localhost:9092"):
    producer = KafkaProducer(bootstrap_servers = bootstrap_server, value_serializer = lambda v : json.dumps(v).encode('utf-8'))
    producer.send(topicname, value=data)
    print(f"Data sent to topic  `{topicname}` : {data}")

    producer.flush()
    producer.close()

def consume_data_from_topic(topicname, bootstrap_server = "localhost:9092"):
    consumer = KafkaConsumer(
        topicname, 
        bootstrap_servers = bootstrap_server, 
        value_deserializer = lambda x : json.loads(x.decode('utf-8')),
        auto_offset_reset = "earliest",
        group_id = "my_group"
        )
    print(f"Listening for messages on topic `{topicname}`...")
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    finally:
        consumer.close()

# def produce_live_data(topicname):
#     count = 0
#     while True:
#         data 