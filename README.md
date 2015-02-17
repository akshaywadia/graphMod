## EphGraph 
Incremental shortest distance algorithm for Spark.

##### Overview
The shortest distance between nodes in a weighted, undirected graph is an important metric that has numerous applications. Classical algorithms, like [Dijkstra](http://en.wikipedia.org/wiki/Dijkstra's_algorithm), do not scale to large graphs. Moreover, many interesting real world graphs are *dynamic* and change their structure in real-time. Depending on graph structure, these updates might actually affect only a small portion of the graph. Thus, it is wasteful to re-compute shortest distances over the entire graph after updates.

**Current Spark Implementation** Spark has a native graph handling library called GraphX, which implements a shortest distance algorithm. However, that implementation is not *incremental*, and it needs to be run on the entire graph after an update.

**Incremental Shortest Distance for Spark** The goal of this project is to implement an incremental shortes distance algorithm for Spark. The intuition is that if an update only affects a small portion of the graph, then we only need to recompute on the affected subgraph. This may lead to significant computational savings. This intuition is formalized in the paper [Facilitating Real-Time Graph Mining](
http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.296.654&rep=rep1&type=pdf) by Cai, Logothetis, and Siganos (Cloud Data Management, 2012). The authors give an incremental algorithm for general message-passing graph-parallel frameworks. Here, I implement this algorithm in Scala for Spark.

##### Files and Directories:
- IncrementalSD/ : Implementation of the incremental shortest distance algorithm for Spark.
- ephgraph/ : A simplified implementation -- simplification based on using a specific case of the algorithm that works for shortest distances only.
- spark-stream/ : Spark streaming application for consuming real-time edge updates from a Kafka queue, and computing shortest distances incrementally.
- testing/ : Testing directory, for evaluation and metrics.

##### Usage
The main class is `graphMod`, and can be initialized as:
```Scala
val gmod = graphMod()
```

The graph type that graphMod works with is `Graph[VertexId,Double]` where the edge weight type is `Double`. First call `initVattr()` to initialize vertex data, and converte graph to correct type.
```Scala
val gr : Graph[VertexId,Double] = ...
val grInitialized = gmod.initVattr(gr)
```

To run incremental shortest distance, call run:
```Scala 
val grSD = gmod.run(grInitialized)
```

To update the weight of an edge, use `updateEdge()`:
```Scala
val grUpdated = gmod.updateEdge(edge, grInitialized)
```
Here, `edge` is a `String` of the form `"src dst wt"`. There is (slower) batch version of `updateEdge()`, called `updateEdgeBatch()`, which takes as input an `RDD[String]` of edges to be updated.
