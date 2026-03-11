Questions:
What partition did most orders go to? Why?

Most orders went to partition 2 and partition 0 This happens in this case since we are hashing the same 10 order ids over and over, and only one gets assigned to partition 1, which leads to it being mostly empty.

What happens if you restart the consumer? Does it re-read messages?

The consumer keeps track of where it has read to, and due to that you will never see it read a message it already has went through once.

What is the highest offset for each partition?

Partition 0 : 31
Partition 1 : 7
Partition 2 : 39


==================================================
ORDER PRODUCER
==================================================
Sending 10 orders to topic 'orders-exercise'...
--------------------------------------------------
    Order ORD-01 sent to partition 0, offset 20
    Order ORD-02 sent to partition 2, offset 25
    Order ORD-03 sent to partition 2, offset 26
    Order ORD-04 sent to partition 0, offset 21
    Order ORD-05 sent to partition 0, offset 22
    Order ORD-06 sent to partition 0, offset 23
    Order ORD-07 sent to partition 2, offset 27
    Order ORD-08 sent to partition 2, offset 28
    Order ORD-09 sent to partition 2, offset 29
    Order ORD-010 sent to partition 1, offset 5
--------------------------------------------------
All 10 orders sent successfully!

==================================================
PRODUCER COMPLETE
==================================================
==================================================
ORDER PRODUCER
==================================================
Sending 10 orders to topic 'orders-exercise'...
--------------------------------------------------
    Order ORD-01 sent to partition 0, offset 24
    Order ORD-02 sent to partition 2, offset 30
    Order ORD-03 sent to partition 2, offset 31
    Order ORD-04 sent to partition 0, offset 25
    Order ORD-05 sent to partition 0, offset 26
    Order ORD-06 sent to partition 0, offset 27
    Order ORD-07 sent to partition 2, offset 32
    Order ORD-08 sent to partition 2, offset 33
    Order ORD-09 sent to partition 2, offset 34
    Order ORD-010 sent to partition 1, offset 6
--------------------------------------------------
All 10 orders sent successfully!

==================================================
PRODUCER COMPLETE
==================================================
==================================================
ORDER CONSUMER
==================================================

Connecting to topic 'orders-exercise'...
Waiting for orders... (Press Ctrl+C to stop)
==================================================

Received Order:
------------------------------
Order ID: ORD-04
Customer: CUST390
Product: Headphones
Quantity: 3
Price: 149.99
Partition: 0, Offset: 17

Received Order:
------------------------------
Order ID: ORD-05
Customer: CUST456
Product: Headphones
Quantity: 4
Price: 149.99
Partition: 0, Offset: 18

Received Order:
------------------------------
Order ID: ORD-06
Customer: CUST697
Product: Mouse
Quantity: 4
Price: 29.99
Partition: 0, Offset: 19

Received Order:
------------------------------
Order ID: ORD-010
Customer: CUST437
Product: Monitor
Quantity: 4
Price: 349.99
Partition: 1, Offset: 4

Received Order:
------------------------------
Order ID: ORD-02
Customer: CUST407
Product: Monitor
Quantity: 5
Price: 349.99
Partition: 2, Offset: 20

Received Order:
------------------------------
Order ID: ORD-03
Customer: CUST628
Product: Mouse
Quantity: 5
Price: 29.99
Partition: 2, Offset: 21

Received Order:
------------------------------
Order ID: ORD-07
Customer: CUST794
Product: Laptop
Quantity: 2
Price: 999.99
Partition: 2, Offset: 22

Received Order:
------------------------------
Order ID: ORD-08
Customer: CUST928
Product: Monitor
Quantity: 2
Price: 349.99
Partition: 2, Offset: 23

Received Order:
------------------------------
Order ID: ORD-09
Customer: CUST482
Product: Keyboard
Quantity: 2
Price: 79.99
Partition: 2, Offset: 24

Received Order:
------------------------------
Order ID: ORD-01
Customer: CUST779
Product: Headphones
Quantity: 4
Price: 149.99
Partition: 0, Offset: 20

Received Order:
------------------------------
Order ID: ORD-02
Customer: CUST242
Product: Headphones
Quantity: 5
Price: 149.99
Partition: 2, Offset: 25

Received Order:
------------------------------
Order ID: ORD-03
Customer: CUST248
Product: Mouse
Quantity: 5
Price: 29.99
Partition: 2, Offset: 26

Received Order:
------------------------------
Order ID: ORD-04
Customer: CUST195
Product: Laptop
Quantity: 5
Price: 999.99
Partition: 0, Offset: 21

Received Order:
------------------------------
Order ID: ORD-05
Customer: CUST742
Product: Monitor
Quantity: 4
Price: 349.99
Partition: 0, Offset: 22

Received Order:
------------------------------
Order ID: ORD-06
Customer: CUST580
Product: Keyboard
Quantity: 3
Price: 79.99
Partition: 0, Offset: 23

Received Order:
------------------------------
Order ID: ORD-07
Customer: CUST870
Product: Mouse
Quantity: 2
Price: 29.99
Partition: 2, Offset: 27

Received Order:
------------------------------
Order ID: ORD-08
Customer: CUST527
Product: Keyboard
Quantity: 4
Price: 79.99
Partition: 2, Offset: 28

Received Order:
------------------------------
Order ID: ORD-09
Customer: CUST270
Product: Mouse
Quantity: 5
Price: 29.99
Partition: 2, Offset: 29

Received Order:
------------------------------
Order ID: ORD-010
Customer: CUST681
Product: Laptop
Quantity: 4
Price: 999.99
Partition: 1, Offset: 5

Received Order:
------------------------------
Order ID: ORD-01
Customer: CUST123
Product: Headphones
Quantity: 3
Price: 149.99
Partition: 0, Offset: 24

Received Order:
------------------------------
Order ID: ORD-02
Customer: CUST237
Product: Keyboard
Quantity: 3
Price: 79.99
Partition: 2, Offset: 30

Received Order:
------------------------------
Order ID: ORD-03
Customer: CUST316
Product: Headphones
Quantity: 1
Price: 149.99
Partition: 2, Offset: 31

Received Order:
------------------------------
Order ID: ORD-04
Customer: CUST577
Product: Mouse
Quantity: 5
Price: 29.99
Partition: 0, Offset: 25

Received Order:
------------------------------
Order ID: ORD-05
Customer: CUST274
Product: Monitor
Quantity: 2
Price: 349.99
Partition: 0, Offset: 26

Received Order:
------------------------------
Order ID: ORD-06
Customer: CUST112
Product: Monitor
Quantity: 3
Price: 349.99
Partition: 0, Offset: 27

Received Order:
------------------------------
Order ID: ORD-07
Customer: CUST564
Product: Monitor
Quantity: 5
Price: 349.99
Partition: 2, Offset: 32

Received Order:
------------------------------
Order ID: ORD-08
Customer: CUST342
Product: Mouse
Quantity: 4
Price: 29.99
Partition: 2, Offset: 33

Received Order:
------------------------------
Order ID: ORD-09
Customer: CUST297
Product: Headphones
Quantity: 1
Price: 149.99
Partition: 2, Offset: 34

Received Order:
------------------------------
Order ID: ORD-010
Customer: CUST652
Product: Keyboard
Quantity: 5
Price: 79.99
Partition: 1, Offset: 6


Shutting down consumer...
==================================================

Consumed 0 orders total

==================================================
CONSUMER COMPLETE
==================================================
==================================================
ORDER PRODUCER
==================================================
Sending 10 orders to topic 'orders-exercise'...
--------------------------------------------------
    Order ORD-01 sent to partition 0, offset 28
    Order ORD-02 sent to partition 2, offset 35
    Order ORD-03 sent to partition 2, offset 36
    Order ORD-04 sent to partition 0, offset 29
    Order ORD-05 sent to partition 0, offset 30
    Order ORD-06 sent to partition 0, offset 31
    Order ORD-07 sent to partition 2, offset 37
    Order ORD-08 sent to partition 2, offset 38
    Order ORD-09 sent to partition 2, offset 39
    Order ORD-010 sent to partition 1, offset 7
--------------------------------------------------
All 10 orders sent successfully!

==================================================
PRODUCER COMPLETE
==================================================
