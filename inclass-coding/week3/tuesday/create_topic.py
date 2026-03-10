from kafka.admin import KafkaAdminClient, NewTopic

def main():
    #Goal -> Create a topic that can handle our website clicks
    #config -> I can control the name, number of partitions, replication factor, retention period, delete policy

    admin = KafkaAdminClient(
        bootstrap_servers = "localhost:9092",
        client_id = "clickstream-demo"
    )

    #Create a new topic
    topic = NewTopic(
        name="website_clicks",#Name of the topic thats being connected to
        num_partitions=3, #Controls the number of of partitions
        replication_factor=1 # Replication factor of 1 means no replicas
    )

    #Use the admin client 

    admin.create_topics([topic])

if __name__ == "__main__":
    main()