import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger 
import org.apache.log4j.Level 


object Drv {
  def main(args : Array[String]) {
    // Create SparkContext -- boilerplate
    val conf = new SparkConf().setAppName("Drv")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF) 
    /* Read graph data from input file, and create raw RDD.
     * Input graph format: one edge per line, where each line is a 
     * string "srcId dstId weight".
     * 'file' is set to a MappedRDD object.
     */

    val gmod = new ephGraphMetrics()
    val gr = gmod.naryTree(5,2,sc)
    gr.vertices.saveAsTextFile("/user/akshay/delta/v/")
    gr.edges.saveAsTextFile("/user/akshay/delta/e/")

    // read graph from file
/*    val gmod = new ephGraphMetrics()
    val gr = gmod.readFromFile("/user/akshay/z.gr", sc)
    val grSD = gmod.run(gr)
    val grUpdated = gmod.createEdges("/user/akshay/sampEds.txt", sc, grSD)
    //println("Starting reset...")
    //grReset.vertices.saveAsTextFile("/user/akshay/delta/v")
    //println("Reset done, starting run ...")

    //System.err.println(grReset.vertices.collect.mkString("\n"))
    val grSDU = gmod.run(grUpdated)
    println("Run done, starting count ...")
    System.err.println("Count = " + gmod.countDisturbed(grSDU).toInt)
    println("Count done.")
    //grSDU.vertices.saveAsTextFile("/user/akshay/delta/v")
    //grSDU.edges.saveAsTextFile("/user/akshay/delta/e")  
*/
  }

}
/* <-----------------
    
    
    val file = sc.textFile("/user/akshay/email-wt.txt")


    //  Map 'file' to EdgeRDD, by parsing records appropriately.
    val edrdd = file.map{ed =>
      val comps = ed.split(" ")
      new Edge(comps(0).toInt, comps(1).toInt, comps(2).toDouble) }

    // Create graph from EdgeRDD
    var gr = Graph.fromEdges(edrdd, 0)

    val gmod = new ephGraph()

    /// pregel ///
//    val initMsg = Map[VertexId, Double]()
    val grInit  = gr.mapVertices{ (vid,vattr) => 
      val newDist = if (vid == 0) 0 else Double.MaxValue
      gmod.Memo(newDist,Map[VertexId,Double]())} 
    //var g = grInit.mapVertices( (vid,vattr) => gmod.vertexProgram(vid,vattr,initMsg))
   // var msg = g.mapReduceTriplets(gmod.sendMessage, gmod.mergeMsgs)
    //g.vertices.saveAsTextFile("/user/akshay/delta/v")
    //g.edges.saveAsTextFile("/user/akshay/delta/e")
    //msg.saveAsTextFile("/user/akshay/delta/m")

    //val grInit  = gr.mapVertices{ (vid,vattr) => gmod.Memo(Double.MaxValue,Map[VertexId,Double]())}
    val grSD = gmod.run(grInit)
    System.err.println("Starting updates... \n")
    val grUp = gmod.updateEdge("0 736 0.115", grSD)
    val grFinal = gmod.run(grUp)
    gmod.saveToText("/user/akshay/delta/d2", grFinal) 
     
    //grSD.vertices.map{ case (vid,vattr) => (vid,vattr.distSoFar)}.saveAsTextFile("/user/akshay/delta/r")

  }
}

---------------> */
