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

  /* Messge digest. Stores all incoming messages, indexed by VertexId, for a particular stage.
   */
  type MsgDigest = Map[VertexId,Double]

  /* The memoization mechanism. Maps stage number to message digest received in that 
   * stage.  */
  case class MemoInfo(memoDist : Double, memoMessages : MsgDigest)
  type Memo = Map[Int,MemoInfo]

  /* Vertex attribute type -- a Tuple4, where the first component is a flag 
   * indicating whether vertex should participate or not, the second component 
   * is the stage number, the third is the shortest path length so far, and 
   * the fourth component is the Memo. */
  case class Vattr(
    affected : Boolean,
    globalStage : Int,
    vertexStage : Int,
    participate : Boolean,
    distSoFar : Double,
    memo : Memo )

  private def initMemo() : Memo = Map[Int,(Double,Map[VertexId,Double]())]()

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

  private def mergeMsgs(memoMsg : MsgDigest, newMsg : MsgDigest) : MsgDigest = 
    memoMsg ++ newMsg

  /* Takes as input attr, which contains previously memoized messages, merges that with
   * new messages received, appends current state, and computes min. */
  private def computeState(attr : Vattr, messages : MsgDigest) : Double = {
    val mergedMessages = mergeMsgs(attr.memo(attr.globalStage).memoMessages, messages)
    return (attr.distSoFar :: mergedMessages.values.toList).reduce((a,b) => math.min(a,b))
  }

  /* Demo participate function. */
  private def participate(attr : Vattr, messages : MsgDigest) : Boolean = {
    val memoizedInfo =  attr.memo.getOrElse(attr.globalStage,Map[Int,MemoInfo]())
    // return true if no previously memoized messages
    if (memoizedInfo.isEmpty) return true
    else {
      val memoDist = memoizedInfo.memoDist
      val memoMsgs = memoizedInfo.memoMessages
      val currentDist = computeState(attr, messages)
      if (memoMsgs != messages) return true
      if (currentDist != memoState) return true
      if (attr.affected) return true
      else return false
      
    }

  }

  /* vertexProgram. This is run on every vertex. Recall that this is executed
   * *after* the innerJoin in Pregel. The result of this join is (vid, Vattr, msgDigest).
   * 1. set participation bit
   * 2. update vertex stage
   * 3. merge messages with memoized messages
   * 4. update current shortest distance
   * 5. memoize current state */
  def vertexProgram(id : VertexId, attr : Vattr, messages : MsgDigest) : Vattr = {
    val newParticipate = participate(attr,messages)
    val newVertexStage = attr.globalStage
    val mergedMessages = mergeMsg(attr, messages) // returns map with updated messages
    val newDist = computeState(attr, messages)
    val newMemo = attr.memo + (attr.globalStage -> MemoInfo(
      newDist,
      mergedMessages)
    return Vattr(
      attr.affected, 
      attr.globalStage, 
      newVertexStage, 
      newParticipate, 
      newDist,
      newMemo )
    }

  private def isParticipateCurrent(attr : Vattr) : Boolean = attr.globalStage == attr.vertexStage

  private def activeCurrentStage(attr : Vattr) : Boolean = {
    // Check if vertex was active in the current stage, in the true execution. Check
    // if the memoized version is empty or not.
    return attr.Memo contains attr.globalStage
  }


  /* sendMsg : looks at a triplet, and prepares message for the destination vertex.
   */
  def sendMessage(edge: EdgeTriplet[Vattr,_]) : Iterator[(VertexId, Vattr)] = {
    /* If participation status current, then follow that. Else, check if this vertex is 
     * affected *and* expecting a message. In this case, send message. */
    if (isParticipateCurrent(edge.srcAttr) {
      // check for receiver state optimization.
      if (edge.srcAttr.participate) return Map(srcVertexId -> edge.srcAttr.distSoFar + 1.0)
      else return Map[VertexId,Double]()
    }
    else {
      // check if vertex is affected *and* was supposed to send message.
      if (edge.srcAttr.affected && activeCurrentStage(edge.srcAttr)) 
        return Map(srcVertexId -> edge.srcAttr.distSoFar + 1.0)
      else return Map[VertexId, Double]()
    }

    // change for landmarks
    if (edge.dstAttr._3 > newDist) Iterator((edge.dstId, newDist))
    else Iterator.empty
  }

  //def run()
  
  /* Pregel with stage numbers.
   */

  def run(graph: Graph, initalMessage ...) : Graph = {
    // prepare vertices
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()

    // compute Stage 0 messages.
    var messages = g.mapReduceTriplets(sendMessage, addMaps)
    var activeMessages = messages.count()

    // main loop, decide when to stop -- when no new messages.
    var prevG : Graph = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // receive messages
      var newVerts = g.vertices.innerJoin(messages)(vertexProgram).cache()

      // update graph with new vertices
      prevG  = g
      g = g.outerJoinVertices(newVerts) { (vid, oldopt, newopt) =>
        val attr = newopt.getOrElse(oldopt)
        (attr._1, attr._2+1, attr._3, attr._4) }
      g.cache()

      val oldMessages = messages

      // next round of messages
      messages = g.mapReduceTriplets(sendMessage, addMaps) // check Some(...)
      activeMessages = messages.count()

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
      // count the iteration
      i += 1

    
    } //while 



      
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
