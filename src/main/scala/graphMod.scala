import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


class graphMod {

  /*
   * Updates edges for an existing graph.
   * Params -- 
   * 
   * edge      :   String, with format "srcID dstId weight", which are Long,
   * Long, and Double
   *
   * gr        :   Graphp[Int,Double]
   *
   * @return   :   new graph with updated edge weight.
   */
  
  def updateEdge(edge:String, gr:Graph[Int,Double]) : Graph[Int,Double] = {
    // parse edge into edge components
    val comps = edge.split(" ")
    val src = comps(0).toInt
    val dst = comps(1).toInt
    val attr = comps(2).toDouble

    // update weight
    return gr.mapEdges{ ed =>
      if (ed.srcId == src && ed.dstId == dst) attr else ed.attr
    }
  }

  def shortestPathSimple(gr:Graph[Int,Double]) : Graph[Int,Double] = {
    // From the CloudDB12 paper. -- cite --

  }

  /* TO IMPROVE
   * val spGraph = graph.mapVertices { (vid, attr) =>
   *       if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
   *           }
   */

  /*def constructGraph(edgeFile : String) : Graph =  {
    val conf = new SparkConf().setAppName("graphMod")
    val sc = new SparkContext(conf)
    val gr = GraphLoader.edgeListFile(sc,edgeFile)
    return gr
  } */
}

/*
val file = sc.textFile("/user/3.gr")

val edrdd = data.map{ed =>
  val comps = ed.split(" ")
  new Edge(comps(0).toInt, comps(1).toInt, comps(2).toFloat) }

val gr = Graph.fromEdges(edrdd,0) 

val gr2 = gr.mapEdges(ed => if (ed.srcId == 0) 5 else ed.attr) 

 
 */
