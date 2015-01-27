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

  /**************
   *    ALTERING 
   *    spark/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala
   *
   */

  /* The memoization mechanism. Maps stage number to message received in that 
   * stage. The message recieved is the 'reduced message', that is, the length
   * of the shortes path so far.  */
  type Memo = Map[Int,Double]

  /* Vertex attribute type -- a Tuple4, where the first component is a flag 
   * indicating whether vertex should participate or not, the second component 
   * is the stage number, the third is the shortest path length so far, and 
   * the fourth component is the Memo. */
  type Vattr = (Bool, Int, Double, Memo)

  /* Increment map. Not clear if I will need it. 
   * Note that in my case, SPMap is just a single value, the first component
   * of Vattr. For landmarks, should be generalized to maps.*/
  private def incrementMap(dist : Double) : Double = dist + 1

  /* This is the 'reduce' function. A vertex receives messages from all 
   * neighbours, and the reduce function reduces all incoming messages to 
   * one message. Here, it is simply the min function. For landmarks, it will
   * be as in graphx docs. */
  private def addMaps(dist1 : Double, dist2 : Double) : Double = math.min(dist1,dist2)

  /* This comment intentionally left blank. */
  
  /* Functions used for Pregel interface. */

  /*
   * Reminder: Don't need to send memoized state, that is only local to 
   * each vertex.
   */

  /* vertexProgram. This is run on every vertex. Recall that this is executed
   * *after* the innerJoin in Pregel. So the inputs are (id, oldState, 
   * newState). The result of this join is (vid, Vattr, dist). First, check if
   * this vertex will participate, by comparing dist with memoized message 
   * for this state. If so, set 'participate' bit, and update state. 
   */
  def vertexProgram(id : VertexId, attr : Vattr, msg : dist) : Vattr = {
    if (participate(attr, msg)) {
      val newParticipate = true
      val currentStage = attr._2
      // new memo
      val newMemo = attr._4 + (currentStage -> msg)
      // update dist
      val newDist = msg
      // return new attr
      return (newParticipate, currentStage, newDist, newMemo) }
      else 
        return attr
    }
  }

  /* sendMsg : looks at a triplet, and prepares message for the destination vertex.
   */
  def sendMessage(edge: EdgeTriplet[Vattr,_]) : Iterator[(VertexId, Vattr)] = {
    // for edges of wt 1 at the moment. Edit this for general weights.
    val newDist = edge.srcAttr._3+1

    // change for landmarks
    if (edge.dstAttr._3 > newDist) Iterator((edge.dstId, newDist))
    else Iterator.empty
  }

  //def run()

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
=========================
  == pregel

  type MsgMap = Map[Long,Double]
  type StateMap = Map[Int,MsgMap]

  // this is to define vertex reduce
  def addMap(msgmap1 : MsgMap, msgmap2 : MsgMap) : MsgMap = {
    (msgmap1.keys ++ msgmap2.keys).map {
      k => k -> math.min(msgmap1.getOrElse(k,Int.MaxValue),
        msgmap2.getOrElse(k,Int.MaxValue)) }
  }
 
 */
