import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object profession {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Profession")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // Using vertices method
    // Count all users which are postdocs
    val postdoc_count = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println(s"Count all users which are postdocs = $postdoc_count")

    // Using edges method
    // Count all the edges where src > dst
    val srcId_greater_than_dstId_count = graph.edges.filter(e => e.srcId > e.dstId).count
    println(s"Count all the edges where src > dst = $srcId_greater_than_dstId_count")

    // Same as above but using case class constructor
    val srcId_greater_than_dstId_count_1 = graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    println(srcId_greater_than_dstId_count_1)

    // Using triplets method
    println('\n'+"Graph View - Initial")
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))

    // Out Degrees
    println('\n'+"Outdegrees")
    graph.outDegrees.collect.foreach(println)

    // Using outerJoinVertices
    // Given a graph where the vertex property is the out degree
    println('\n'+"Graph View - After outerJoinVertices ")
    val inputGraph: Graph[PartitionID, String] = graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    inputGraph.triplets.collect.foreach(println)

    // Same as above but with Style
    println('\n'+"Graph View - After outerJoinVertices - Another Way")
    val degreeGraph: Graph[PartitionID, String] = graph.outerJoinVertices(graph.outDegrees) { (id, oldAttr, outDegOpt) =>
      outDegOpt match {
        case Some(outDeg) => outDeg
        case None => 0 // No outDegree means zero outDegree
      }
    }
    degreeGraph.triplets.collect.foreach(println)

    // Using mapTriplets
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank
    println('\n'+"Graph View - Modifying Edge Attribute as 1.0 / src_outDegree and Vertex Attribute as 1.0")
    val outputGraph: Graph[Double, Double] = inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
    outputGraph.triplets.collect.foreach(println)

    // Using Connected Components
    println('\n'+"Graph View - Using Connected Components")
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    ccGraph.triplets.collect.foreach(println)

    // Using subgraph
    // Remove missing vertices as well as the edges to connected to them
    println('\n'+"Graph View - Removing Missing Vertex")
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    validGraph.triplets.collect.foreach(println)

    // Using mask
    // Restrict the answer to the valid subgraph
    println('\n'+"Graph View - Restrict the answer to the valid subgraph")
    val validCCGraph = ccGraph.mask(validGraph)
    validCCGraph.triplets.collect.foreach(println)


  }
}
