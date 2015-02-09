## EphGraph 
Incremental shortest distance algorithm for Spark.

##### Overview
The shortest distance between nodes in a weighted, undirected graph is an important metric that has numerous applications. For example, it can be directly used to rank search results in a social network. Shortest distances for large graphs can be computed effectively in the map-reduce framework, and indeed, the GraphX library in Spark does implement such an algorithm. 

An important feature of real-world large scale networks is that they are *dynamic*. Taking the example of a social network again, the connectivity of the social graph changes constantly as people add new friends. This requires us to constantly refresh the shortest distances. However, recomputing shortest distances over the entire graph after every update is very expensive.

A better solution is to compute shortest distance *incrementally*. Intuitively, depending on the structure of the graph, there can be many updates that only affect a small portion of the graph. If we can identify the sub-graph which is affected by an update, then we can potentially save significant computation.

This intuition is formalized in the paper [Facilitating Real-Time Graph Mining](
http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.296.654&rep=rep1&type=pdf) by Cai, Logothetis, and Siganos (Cloud Data Management, 2012). The authors give an incremental algorithm for general message-passing graph-parallel frameworks.


The goal of this project is to implement the above algorithm for Spark.

##### Files and Directories:
- IncrementalSD/ : Implementation of the incremental shortest distance algorithm for Spark.
- spark-stream/ : Spark streaming application for consuming real-time edge updates from a Kafka queue, and computing shortest distances incrementally.
- testing/ : Testing directory, for evaluation and metrics.
