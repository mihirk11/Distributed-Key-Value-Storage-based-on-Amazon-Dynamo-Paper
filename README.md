# Replicated-Key-Value-Storage-based-on-Amazon-Dynamo-Paper
Amazon dynamo paper based replicated, fault tolerant key distributed key value storage.

The main goal is to provide both availability and linearizability at the same time. In other words, the implementation always performs read and write operations successfully even under failures. 
At the same time, a read operation always returns the most recent value.

Achieving this was made possible by referring Amazon Dynamo paper.

Availability is achieved by using chain replication strategy. While linearizability is achieved by reading from the last node in the replicated chain.
Random failures of any number os nodes are detected and mitigated while providing availability and linearizability.
