package agd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.io.PrintWriter

case class Edge(v: String, adjList: Seq[String])
case class Distance(v: String, dist: Int)
case class Joined(v: String, adjList: Seq[String], dist: Int)

object Spark_DS {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    
    val conf = new SparkConf().setAppName("PageRank RDD")
    conf.set("spark.eventLog.enabled", "true")
//    conf.set("spark.eventLog.dir", "logs")
    val sc = new SparkContext(conf)
    
    val spark: SparkSession = SparkSession.builder
    .config(conf)
    .getOrCreate;

    import spark.implicits._

		// delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    // Store the graph adjacency list into an RDD
    var graph = sc.textFile(args(0)).map(line => line.split("\\s+")).map(nodes => Edge(nodes(0), nodes(1).split(",").toSeq)).toDS.coalesce(4)
    // Tell Spark to try and keep this pair RDD around in memory for efficient re-use
    graph.persist()
    
    // Define an accumulator
    val counter = sc.longAccumulator("counter")
    
    val k = args(2)
    val source = args(3)
    
    // Create the initial distances
    var distances = graph.map(edge => if (edge.adjList.contains(source)) Distance(edge.v, 1) else Distance(edge.v, Int.MaxValue))
    
    // Function extractVertices returns each vertex id m in n's adjacency list as (m, distance(n)+1)
    def extractVertices (joined: Joined) : Seq[Distance] = {
      var res = Seq[Distance]()
      res = res :+ Distance(joined.v, joined.dist)
      for (m <- joined.adjList) {
        if (joined.dist != Int.MaxValue) {
          res = res :+ Distance(m, joined.dist + 1)
        } else {
          res = res :+ Distance(m, joined.dist)
        }
      }
      return res
    }
    
    var numUpdated = 20
    while (numUpdated > 0) {
      distances = graph.joinWith(distances, graph("v") === distances("v"))
        .map(joined => Joined(joined._1.v, joined._1.adjList, joined._2.dist))
        .flatMap(joined => extractVertices(joined))
      val TempDF = distances.groupBy("v")
        .agg(min(distances("dist")))
      distances = TempDF.map{aggregated => Distance(aggregated.getAs[String](0), aggregated.getAs[Int](1))}
      numUpdated = numUpdated - 1
    }
    
    // debug
    print(distances.explain)
    
    // Report the lengths of shortest paths from source to all vertices
    sc.parallelize(distances.collect().toSeq).saveAsTextFile(args(1))
    
    // Report the length of the longest shortest path
    val lsp = (distances.map(row => if (row.dist != Int.MaxValue) Distance(row.v, row.dist) else Distance(row.v, 0))
      .agg(Map("dist" -> "max")).collect())(0)(0)
    //new PrintWriter("lsp.txt") { write(source + " " + lsp.toString()); close }
    logger.setLevel(Level.WARN)
    logger.warn(source + " " + lsp.toString())
    sc.parallelize(Array(lsp)).saveAsTextFile("s3://bucket-spark-ds-agd/ouput2")
    
  }
}

