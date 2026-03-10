# Kafka Architecture Concepts - Answers

## Part 1: Concept Questions

### 1. What is the difference between a topic and a partition?

A topic is like a folder that is used to store papers, while a partition is like a divider in the folder. The partitions maintain structure within themselves, but they are not able to speak on the order of the topic as a whole.


### 2. Why might you choose to have multiple partitions for a single topic?

One of the key reasons would be for parallelism, as you may not want millions of topic entries to be stored on one partition. Multiple partitions for a single topic could also allow you to split up data. For example, if you were tracking what websites people have visited, you may want different paths to be stored in different partitions, like having "/home" stored in one partition and having "/about" be on a different partition. 


### 3. What is the role of ZooKeeper in a Kafka cluster? What is KRaft and how is it different?

ZooKeeper is a separate cluster which allows the tracking and storing of metadata. KRaft on the other hand allows Kafka to track its own metadata, and does not require that extra cluster. ZooKeeper is a bit more dated these days, with KRaft being better for Kafka version 2.8+. Some new versions of Kafka disregard the ZooKeeper approach all together, though it would still be used in some legacy systems.


### 4. Explain the difference between a leader replica and a follower replica.

A leader replica is what is being written to and read to. On the other hand, a follower replica of a topic partition would be kept in the case that the leader breaks and needs to be replaced.


### 5. What does ISR stand for, and why is it important for fault tolerance?

In-sync Replica is what ISR stands for, and it allows for data to be safe by having copies of data in a follower replica in case a leader replica breaks. If the leader crashes, the data is not lost, which is essiential in most industry-grade applications


### 6. How does Kafka differ from traditional message queues like RabbitMQ?

Kafka messages are not inherently sorted, as in messages are not sorted in in terms of a whole topic. Kafka does however allow some ordering of messages in a specific partition within a topic. RabbitMQ moreso orders messages in a queue, while kafka orders messages in a specific partition.


### 7. What is the publish-subscribe pattern, and how does Kafka implement it?

Kafka uses the publish-subscribe pattern, where there are publishers who write to topics, and subscribers who read from a topic. The proper names of the two mentioned groups are producers and consumers, and they are essiential in allowing the separation of data creation and data manipulation. It can be thought of as a YouTube channel, where the creater uploads to a channel and a viewer/subscriber watches the videos. There is no direct connection between those to users of the channel, but the videos are shared.


### 8. What happens when a Kafka broker fails? How does the cluster recover?

Kafka can have a cluster of several brokers, where if one fails another can pick up where the leading broker left off. This is very much inline with the concept of ISR and what it helps to do, but in terms of an entire broker.