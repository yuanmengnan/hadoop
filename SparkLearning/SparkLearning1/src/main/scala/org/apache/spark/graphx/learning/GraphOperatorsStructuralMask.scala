/**
 * @author xubo
 * ref http://spark.apache.org/docs/1.5.2/graphx-programming-guide.html
 * time 20160503
 */

package org.apache.spark.graphx.learning
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object GraphOperatorsStructuralMask {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphOperatorsStructuralMask").setMaster("local[4]")
    // Assume the SparkContext has already been constructed
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    println("vertices:");
    graph.subgraph(each => each.srcId != 100L).vertices.collect.foreach(println)
    println("\ntriplets:");
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println(_))
    graph.edges.collect.foreach(println)

    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph)
    println("\nccGraph:");
    println("vertices:");
    ccGraph.vertices.collect.foreach(println)
    println("edegs:");
    ccGraph.edges.collect.foreach(println)
    println("\nvalidGraph:");
    validGraph.vertices.collect.foreach(println)
    println("\nvalidCCGraph:");
    validCCGraph.vertices.collect.foreach(println)
    sc.stop()
  }

}