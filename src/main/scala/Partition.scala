import org.apache.spark.{SparkConf, SparkContext}

object Partition {

  val depth = 6
  var vt = 1
  val neg: Long = -1

  // Function to create a vertex from a line of input
  def Vertex(s: Array[String]): (Long, Long, List[Long]) = {
    var centroid: Long = -1
    if (vt <= 10) {
      vt = vt + 1
      centroid = s(0).toLong
    }
    (s(0).toLong, centroid, s.tail.map(_.toString.toLong).toList)
  }

  // Mapper function (1)
  def function1(vertex: (Long, Long, List[Long])): List[(Long, Either[(Long, List[Long]), Long])] = {
    var n = List[(Long, Either[(Long, List[Long]), Long])]()
    if (vertex._2 > 0) {
      vertex._3.foreach(x => {
        n = (x, Right(vertex._2)) :: n
      })
    }
    n = (vertex._1, Left(vertex._2, vertex._3)) :: n
    n
  }

  // Function to combine vertices and assign clusters
  def function2(vertex1: (Long, Iterable[Either[(Long, List[Long]), Long]])): (Long, Long, List[Long]) = {
    var adjacent: List[Long] = List[Long]()
    var cluster: Long = -1

    for (ver <- vertex1._2) {
      ver match {
        case Right(c) =>
          // Update cluster if a valid one is found
          if (c > cluster) {
            cluster = c
          }
        case Left((c, ad)) if c > 0 =>
          return (vertex1._1, c, ad) // Return immediately if a valid cluster is found
        case Left((neg, ad)) =>
          adjacent = ad
      }
    }

    // Debugging statement to track the vertex and its assigned cluster
    println(s"Vertex ID: ${vertex1._1}, Assigned Cluster: $cluster, Adjacent Nodes: ${adjacent.mkString(", ")}")

    (vertex1._1, cluster, adjacent)
  }

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val conf = new SparkConf().setAppName("Graph Partitioning").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Load the graph data from the input file
    var graph = sc.textFile(args(0)).map(line => Vertex(line.split(",")))

    // Iteratively update clusters
    for (i <- 1 to depth) {
      graph = graph.flatMap(vertex => function1(vertex))
                   .groupByKey()
                   .map(vertex => function2(vertex))

      // Debugging: log the graph state after each iteration
      println(s"After iteration $i: ${graph.collect().mkString(", ")}")
    }

    // Count cluster sizes
    val m = graph.map(pt => (pt._2, 1))
    val op = m.reduceByKey(_ + _).collect()

    // Print the output
    println("Final Cluster Sizes:")
    op.foreach { case (clusterId, size) => 
      println(s"Cluster ID: $clusterId, Size: $size")
    }

    // Stop the Spark context
    sc.stop()
  }
}
