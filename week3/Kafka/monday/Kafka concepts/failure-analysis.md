# Failure Analysis

## Scenario

Your Kafka cluster has 3 brokers. The "orders" topic has 3 partitions with replication factor 2:

| Partition | Leader | Follower |
|-----------|--------|----------|
| Partition 0 | Broker 1 | Broker 2 |
| Partition 1 | Broker 2 | Broker 3 |
| Partition 2 | Broker 3 | Broker 1 |

**Event: Broker 2 suddenly crashes.**

---

## Analysis Questions

### 1. Which partitions are affected?

Partitions 0 and 1


### 2. What happens to Partition 0?

*Consider: Broker 2 was the follower for Partition 0. What is the impact?*

Since the follower replica of partition 0 is lost, that would make partition 0 no longer remain fault tolerant.


### 3. What happens to Partition 1?

*Consider: Broker 2 was the leader for Partition 1. Who becomes the new leader?*

The follower replica on broker 3 becomes the leader, and that would make partition one no longer remain fault tolerant


### 4. Can producers still send messages to all partitions? Why or why not?

Yes as all of the partitions still have a leader after electing process occurs, making partition 1 have a new leader


### 5. What is the cluster's replication status after the failure?

*Consider: How many replicas does each partition have now? Is the cluster at risk?*

partition 0 : 1 

partition 1 : 1

partition 2 : 2

The cluster is now at rist as the backups for partitions 0 and 1 are now gone


---

## Bonus Question

If Broker 3 also fails immediately after Broker 2, what happens to Partition 1?

Partition 1 would be lost, leaving only partitions 0 and 2, which means we likely lost some data in this example.