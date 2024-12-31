### Project Description: Graph Partitioning using Spark and Scala
### **Key Concepts:**

- **Graph Representation:**
  The graph is represented as an RDD of tuples, where each tuple represents a node in the graph with the format:
  ```scala
  (Long, Long, List[Long])
  ```
  - The first `Long` is the **node ID**.
  - The second `Long` is the **cluster ID**. It is `-1` initially, except for the first 5 nodes that may have predefined cluster IDs.
  - The `List[Long]` is the list of **adjacent node IDs** (the neighbors of the current node).

- **Partitioning Process:**
  The process involves iteratively updating the cluster assignments of each node. In each iteration, a node can be reassigned based on its neighbors' clusters.

### **Steps to Implement:**

1. **Initial Graph Setup:**
   - Read the graph from the input file (either `small-graph.txt` or `large-graph.txt`).
   - The graph nodes should have an initial cluster ID of `-1`, except for the first 5 nodes, which will have predefined cluster IDs.

2. **Iterative Process:**
   - The graph will be partitioned iteratively over a specified number of iterations (`depth`).
   - **Mapper function (1)**: For each node, emit the node itself along with all its neighbors. This allows the information about the node’s cluster ID to be shared with adjacent nodes.
   - **Join operation**: The `join` operation will combine the graph's updated information with the previous graph data.
   - **Mapper function (2)**: In this step, retrieve the new and old cluster assignments for each node.
   - **Final update (function 3)**: Decide whether to retain the old cluster ID or assign a new cluster ID based on whether the current cluster ID is `-1`.

3. **Output the Partition Sizes:**
   - After the iterative partitioning process, the final task is to print the partition sizes. This can be done by counting how many nodes belong to each cluster.

### **Code Structure:**

The following is the detailed implementation for the **Partition.scala** file in **Spark and Scala**:

```scala
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object GraphPartitioning {
  def main(args: Array[String]): Unit = {
    // Initialize SparkContext
    val conf = new SparkConf().setAppName("GraphPartitioning").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Read the graph from the input file (args(0))
    var graph = sc.textFile(args(0)).map { line =>
      val parts = line.split(",")
      // Each line represents a node: (ID, cluster, List of adjacent nodes)
      (parts(0).toLong, parts(1).toLong, parts.drop(2).map(_.toLong).toList)
    }

    // Initially, set cluster IDs to -1, except for the first 5 nodes
    graph = graph.zipWithIndex.map { case ((id, cluster, adj), idx) =>
      if (idx < 5) (id, idx.toLong, adj) else (id, -1L, adj)
    }

    // Number of iterations (depth) for the partitioning process
    val depth = 10

    for (i <- 1 to depth) {
      // Step 1: Map each node to its cluster and its neighbors
      val mappedGraph = graph.flatMap { case (id, cluster, adj) =>
        val listBuffer = ListBuffer((id, cluster))
        adj.foreach(neighbor => listBuffer += (neighbor, cluster))
        listBuffer
      }

      // Step 2: Reduce the graph by taking the max cluster ID for each node
      val reducedGraph = mappedGraph.reduceByKey(math.max)

      // Step 3: Join the reduced graph with the original graph to update cluster IDs
      graph = reducedGraph
        .join(graph.map { case (id, cluster, adj) => (id, (cluster, adj)) })
        .map { case (id, ((newCluster, _), (oldCluster, adj))) =>
          // Retain the old cluster if it's not -1, otherwise use the new cluster
          val updatedCluster = if (oldCluster == -1) newCluster else oldCluster
          (id, updatedCluster, adj)
        }

      // Optionally: Print the updated graph to see the cluster assignments
      // graph.collect().foreach(println)
    }

    // Step 4: Count the partition sizes (cluster sizes)
    val clusterSizes = graph.map { case (id, cluster, _) => (cluster, 1) }
                            .reduceByKey(_ + _)
                            .collect()

    // Print the partition sizes
    clusterSizes.foreach { case (cluster, size) =>
      println(s"Cluster $cluster has $size nodes.")
    }

    // Stop SparkContext
    sc.stop()
  }
}
```

### **Explanation of the Code:**

1. **Reading the Graph:**
   - The graph is read from the input file (`args(0)`) and parsed into an RDD of tuples `(Long, Long, List[Long])`, where each tuple represents a node with its ID, cluster ID, and list of neighbors.

2. **Initial Cluster Assignment:**
   - The first 5 nodes are assigned unique cluster IDs (0, 1, 2, 3, and 4), while all other nodes are assigned a cluster ID of `-1`.

3. **Iterative Partitioning Process:**
   - **Step 1 (Mapping)**: For each node, we return a pair for the node itself `(id, cluster)` and for each of its neighbors `(neighbor, cluster)`.
   - **Step 2 (Reduction)**: We then reduce by key, keeping the maximum cluster ID for each node (this propagates the cluster ID from neighbors).
   - **Step 3 (Join and Update)**: We join the updated cluster assignments with the original graph. If the original cluster ID is `-1`, we update it with the new cluster ID. Otherwise, the cluster ID remains unchanged.

4. **Output the Cluster Sizes:**
   - After the iterations, the final step is to count how many nodes are in each cluster using `reduceByKey(_ + _)` to aggregate the nodes by their cluster ID.
   - Finally, the partition sizes (i.e., the number of nodes in each cluster) are printed.

### **Execution:**

1. **Build the Program:**
   - Compile the Scala project on Expanse using the appropriate build scripts.

2. **Run the Program:**
   - **In Local Mode (for small data):**
     ```bash
     sbatch partition.local.run
     ```
   - **In Distributed Mode (for large data):**
     ```bash
     sbatch partition.distr.run
     ```

3. **Verify Output:**
   - Ensure that the output cluster sizes match the expected results. The results should show how many nodes belong to each cluster after the iterative partitioning process.

### **Conclusion:**
This approach efficiently partitions the graph using Spark's RDD operations, iteratively updating the cluster assignments. The program operates in both local and distributed modes, ensuring scalability for large graphs.
