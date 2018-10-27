package agd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.io.PrintWriter

object Spark_RDD {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    
    val conf = new SparkConf().setAppName("PageRank RDD")
    conf.set("spark.eventLog.enabled", "true")
//    conf.set("spark.eventLog.dir", "logs")
    val sc = new SparkContext(conf)

		// delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    // Store the graph adjacency list into an RDD
    var graph = sc.textFile(args(0)).map(line => line.split("\\s+")).map(nodes => (nodes(0), nodes(1).split(",")))
    graph = graph.repartition(4)
    // Tell Spark to try and keep this pair RDD around in memory for efficient re-use
    graph.persist()
    
    // Define an accumulator
    val counter = sc.longAccumulator("counter")
    
    val k = args(2)
    val source = args(3)
    
    // Create the initial distances
    var distances = graph.mapValues(nodes => if (nodes contains source) 1 else Int.MaxValue)
    
    // Function extractVertices returns each vertex id m in n's adjacency list as (m, distance(n)+1)
    def extractVertices (tuple: (String, (Array[String], Int))) : Array[(String, Int)] = {
      var res = Array[(String, Int)]()
      res = res :+ (tuple._1, tuple._2._2)
      for (m <- tuple._2._1) {
        if (tuple._2._2 != Int.MaxValue) {
          res = res :+ (m, tuple._2._2 + 1)
        } else {
          res = res :+ (m, tuple._2._2)
        }
      }
      return res
    }
    
    var numUpdated = 20
    while (numUpdated > 0) {
      counter.add(-counter.value)
      distances = graph.join(distances)
        .flatMap(tuple => extractVertices(tuple))
        //Accumulators do not change the lazy evaluation model of Spark.
        .reduceByKey((x, y) => if (x < y) {counter.add(1); x} else if (x > y) {counter.add(1); y} else y) // Remember the shortest of the distances found
      //numUpdated = counter.value
      numUpdated = numUpdated - 1
    }
    
    // debug
    print(distances.toDebugString)
    
    // Report the lengths of shortest paths from source to all vertices
    distances.saveAsTextFile(args(1))
    
    // Report the length of the longest shortest path
    val lsp = distances.map(tuple => if (tuple._2 != Int.MaxValue) tuple._2 else 0).max()
    //new PrintWriter("lsp.txt") { write(source + " " + lsp.toString()); close }
    logger.setLevel(Level.WARN)
    logger.warn(source + " " + lsp.toString())
    sc.parallelize(Array(lsp)).saveAsTextFile("s3://bucket-spark-rdd-agd/ouput2")
    
  }
}