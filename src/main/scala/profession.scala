import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object profession {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Social Network Analysis")
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
    val graph = Graph(users, relationships, defaultUser)

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
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))


  }
}
