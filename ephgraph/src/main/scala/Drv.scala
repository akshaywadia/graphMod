import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Drv {
  def main(args : Array[String]) {
    // Create SparkContext -- boilerplate
    val conf = new SparkConf().setAppName("graphMod")
    val sc = new SparkContext(conf)

    /* Read graph data from input file, and create raw RDD.
     * Input graph format: one edge per line, where each line is a 
     * string "srcId dstId weight".
     * 'file' is set to a MappedRDD object.
     */
    val file = sc.textFile("/user/tree1024.gr")


    //  Map 'file' to EdgeRDD, by parsing records appropriately.
    val edrdd = file.map{ed =>
      val comps = ed.split(" ")
      new Edge(comps(0).toInt, comps(1).toInt, comps(2).toDouble) }

    // Create graph from EdgeRDD
    var gr = Graph.fromEdges(edrdd, 0)

    val gmod = new ephGraph()
    val grInit  = gr.mapVertices{ (vid,vattr) => gmod.Memo(Double.MaxValue,Map[VertexId,Double]())}
    val grSD = gmod.run(grInit)
    gmod.saveToText("/user/delta/", grSD)
     
    //grSD.vertices.map{ case (vid,vattr) => (vid,vattr.distSoFar)}.saveAsTextFile("/user/akshay/delta/r")

  }
}
