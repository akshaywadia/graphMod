import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


class graphMod extends java.io.Serializable {

  /****
   * TYPE DECLARATIONS for storing state information for each vertex.
   ****/

  /* Messge digest. Stores all incoming messages, indexed by VertexId, for a particular stage.
   */
  type MsgDigest = Map[VertexId,Double]

  /* The memoization mechanism. Maps stage number to message digest received in that 
   * stage.  */
  case class MemoInfo(memoDist : Double, memoMessages : MsgDigest)
  type Memo = Map[Int,MemoInfo]


  /* Vertex attribute type --
   *      disturbed   :   whether this vertex was disturbed in this incremental stage or not
   *      affected    :   whether graph update affects this vertex or not
   *      globalStage :   global stage number
   *      vertexStage :   the last stage vertex was active in
   *      participate :   will this vertex send message in this stage (used to signal 
   *                      map-reduce job)
   *      distSoFar   :   distance so far
   *      memo        :   Memo
   */
  case class Vattr(
    disturbed : Int,
    affected : Boolean,
    globalStage : Int,
    vertexStage : Int,
    participate : Boolean,
    distSoFar : Double,
    memo : Memo )

  /****
   * GRAPH UPDATE FUNCTIONS
   ****/

  /*
   * Updates edges for an existing graph.
   *        edge      :   String, with format "srcID dstId weight", which are Long,
   *                      Long, and Double
   *        gr        :   Graphp[Vattr,Double], the post first-run graph.
   *        *return*  :   new graph with updated edge weight.
   */
  
  def updateEdge(edge:String, gr:Graph[Vattr,Double]) : Graph[Vattr,Double] = {
    // parse edge into edge components
    val comps = edge.split(" ")
    val src = comps(0).toLong
    val dst = comps(1).toLong
    val attr = comps(2).toDouble

    // update weight
    val updatedEd = gr.mapEdges{ ed =>
      if (ed.srcId == src && ed.dstId == dst) attr else ed.attr
    }
    // update affectedness

    if (src == 0) // root
      return updatedEd.mapVertices{ case (vid,vattr) => if (vid == src) Vattr(vattr.disturbed, true,
        vattr.globalStage,
        vattr.vertexStage,
        true,
        vattr.distSoFar,
      vattr.memo) else vattr }
      else 
        return updatedEd.mapVertices{ case (vid,vattr) => if (vid == src) Vattr(vattr.disturbed, true,
          vattr.globalStage,
          vattr.vertexStage,
          vattr.participate,
          vattr.distSoFar,
        vattr.memo) else vattr }

  }

  /*
   * Batch udpate of edges.
   *        edges    :   RDD[String] of the form "src dest wt"
   *        gr       :   Post-first run graph
   *        *return* :   Updated graph
   */

  def updateEdgeBatch(edges:RDD[String], gr:Graph[Vattr,Double]) : Graph[Vattr,Double] = {
    var g = gr
    // This is wasteful, figure out correct partitioning strategy.
    val edArray = edges.collect()
    for (line <- edArray)
      g = updateEdge(line,g)
    return g
  }

  /****
   * INCREMENTAL SHORTEST DISTANCE
   * Algorithm taken from:
   *         Zhuhua Cai, Dionysios Logothetis, and Georgos Siganos. 
   *         "Facilitating real-time graph mining." 
   *         Proceedings of the fourth international workshop on Cloud data management. 
   *         ACM, 2012.
   *
   * Code built upon existing (static) shortest path implementation in GraphX:
   * 
   * https://github.com/apache/spark/blob/4a171225ba628192a5ae43a99dc50508cf12491c/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala
   *
   * @TODO find a better way to reference above file.
   *   
   ****/

  /* Increment map. Not clear if I will need it. 
   * Note that in my case, SPMap is just a single value, the first component
   * of Vattr. For landmarks, should be generalized to maps.*/
  private def incrementMap(dist : Double) : Double = dist + 1

  /* This is the 'reduce' function. A vertex receives messages from all 
   * neighbours, and the reduce function reduces all incoming messages to 
   * one message. Here, it is simply the min function. For landmarks, it will
   * be as in graphx docs. 
   *          -- NOT NEEDED, because of change in msg data structure,
   *                         use mergeMsgs()
   * */
  //private def addMaps(dist1 : Double, dist2 : Double) : Double = math.min(dist1,dist2)


  /* Functions used for Pregel-ish interface. */

 /* Reminder: Don't need to send memoized state, that is only local to 
   * each vertex.
   */


  /* The main reduce function for mapReduceTriplets
   * Takes two MsgDigests, and merges them. Note that the '++' operator adds new keys if they
   * don't exist, or updates values if keys exist.
   */
  private def mergeMsgs(memoMsg : MsgDigest, newMsg : MsgDigest) : MsgDigest = 
    memoMsg ++ newMsg




  /* Compute state, that is, the current shortest distance. 
   *        attr      :     Current vertex attributes, use this to obtain 
   *                        previously memoized msgs
   *        messages  :     Received msgs in this stage (vid -> double)
   *        *return*  :     New shortest distance
   * Get memoized msgs, merge with new msgs, and recompute min distance --
   * note that we need to recompute from merges msgs (i.e., no need to consider
   * current state) because the current shortest distance is immutable.
   */
  private def computeState(attr : Vattr, messages : MsgDigest) : Double = {
    val msgDigest = if (attr.memo contains attr.globalStage) 
                    attr.memo(attr.globalStage).memoMessages 
                    else Map[Long,Double]()

    val mergedMessages = mergeMsgs(msgDigest, messages)

    return (mergedMessages.values.toList).reduce((a,b) => math.min(a,b))
  }




  /* Computes whether vertex needs to participate in the current stage or not.
   * For explanation of coniditons, see Sec 3.2 of the above paper.
   *
   * NB: This is not exhaustive. There are situations where a vertex will participate
   * even if it's vertexProgram is not entered. This happens in the INCREMENTAL stage, 
   * when this vertex was affected *and* it was supposed to receive messages. This
   * case is handled in sendMessages.
   */
  private def participate(attr : Vattr, messages : MsgDigest) : Boolean = {
    val memo = attr.memo
    // if received new messages, and no previous memo state, participate
    if (!(memo contains attr.globalStage)) return true
    else {  // return true if no previously memoized messages
      val memoInfo = memo(attr.globalStage)
      val memoDist = memoInfo.memoDist
      val memoMsgs = memoInfo.memoMessages
      val currentDist = computeState(attr, messages)
      if (memoMsgs != messages) return true
      if (currentDist != memoDist) return true
      if (attr.affected) return true
      else return false
      
    }
  }

  /* vertexProgram. This is run only for vertices that recieve input messages.
   * Recall that this is executed
   * *after* the innerJoin in Pregel. The result of this join is (vid, Vattr, msgDigest).
   * 1. set participation bit
   *    1.5. set disturbed bit // debug only
   * 2. update vertex stage
   * 3. merge messages with memoized messages
   * 4. update current shortest distance
   * 5. memoize current state */
  def vertexProgram(id : VertexId, attr : Vattr, messages : MsgDigest) : Vattr = {

    val newParticipate = participate(attr,messages) 
    val newDisturbed = if (newParticipate) 1 else attr.disturbed
    
    //this is getting ready to participate in the *next* stage.
    val newVertexStage = (attr.globalStage+1)
    val memoizedMsgDigest  = if (attr.memo contains attr.globalStage) attr.memo(attr.globalStage).memoMessages else Map[Long,Double]()
    
    val mergedMessages = mergeMsgs(memoizedMsgDigest,messages) // returns map with updated messages
    val newDist = computeState(attr, messages)
    val newMemo = attr.memo + (attr.globalStage -> MemoInfo(
      newDist,
      mergedMessages))
    return Vattr(
      newDisturbed,
      attr.affected, 
      attr.globalStage, 
      newVertexStage, 
      newParticipate, 
      newDist,
      newMemo ) 
    }

  /** HELPER FUNCTIONS for sendMessage **/

  /* Check if participation status current or not
   */
  private def isParticipateCurrent(attr : Vattr) : Boolean = attr.globalStage == attr.vertexStage


  /* Check if vertex active in the current state in NORMAL operation.
   */
  private def activeCurrentStage(attr : Vattr) : Boolean = {
    // Check if the memoized version is empty or not.
    return attr.memo contains attr.globalStage-1
  }


  /* sendMsg : looks at a triplet, and prepares message for the destination vertex.
   */
  def sendMessage(edge: EdgeTriplet[Vattr,Double]) : Iterator[(VertexId, MsgDigest)] = {

    val gstage = edge.srcAttr.globalStage

    // handle initial stage first, where only root is active
    if (gstage == 0) {
        if (edge.srcAttr.participate) 
          return Iterator((edge.dstId,Map(edge.srcId -> (edge.srcAttr.distSoFar + edge.attr))))
        else return Iterator.empty
    }
    else {
      // If participation status current, then follow that. 
      if (isParticipateCurrent(edge.srcAttr)) {
        if (edge.srcAttr.participate) return Iterator((edge.dstId,Map(edge.srcId -> (edge.srcAttr.distSoFar + edge.attr))))
        else return Iterator.empty
      }
      else {
        // check if vertex is affected *and* was supposed to send message.
        if (edge.srcAttr.affected && activeCurrentStage(edge.srcAttr)) 
          return Iterator((edge.dstId,Map(edge.srcId -> (edge.srcAttr.distSoFar + edge.attr))))
        else return Iterator.empty
      }
    }
  }


  /* Initialize vertices and convert Graph[Int,Double] to Graph[Vattr,Double]
   */
  def initVattr(gr : Graph[Int,Double]) : Graph[Vattr,Double] = {
    val initVertexMsg = Vattr(0,false,0,0,false,Double.MaxValue,Map[Int,MemoInfo]())
    val initVertexMsgSource = Vattr(0,false,0,0,true,0.0,Map[Int,MemoInfo]())
    // serializable issue
    val setVertexAttr = (vid :VertexId, vdata : Int) => if (vid ==0) initVertexMsgSource else initVertexMsg
    return gr.mapVertices(setVertexAttr)
  }


  /* reset graph. To be called *after* a run, and *before* edgeUpdate
   */
  def resetGraph(gr : Graph[Vattr,Double]) : Graph[Vattr,Double] = {
    gr.mapVertices{ case (vid,vattr) => Vattr(0, // disturbed
      false,  // affected
      0,      // globalStage : Int,
      0,      // vertexStage : Int,
      false,  //participate : Boolean,
      vattr.distSoFar,  // distSoFar : Double,
    vattr.memo ) }       // : Memo )
  }

  //def run()

  /* Pregel with stage numbers.
  */

 def run(graph: Graph[Vattr,Double], 
   dbg : Boolean = false,
   maxIterations : Int = 10)
 /*    activeDirection : EdgeDirection = EdgeDirectino.Either)
   (vertexProg : (VertexId, Vattr, MsgDigest) => Vattr,
     sendMsg : EdgeTriplet[Vattr,_] => Iterator[(VertexId,Vattr)],
     mergeMsg : (MsgDigest, MsgDigest) => MsgDigest) */
  : Graph[Vattr,Double] = {


    // NOT NEEDED
    //var g = graph.mapVertices((vid, vdata) => if (vid == 0) initVertexMsgSource
    //else initVertexMsg).cache()

    // compute Stage 0 messages.
    //var messages = g.mapReduceTriplets(sendMessage, mergeMsgs)
    //var activeMessages = messages.count()



    // main loop, decide when to stop -- when no new messages.
    var g = graph
    g.cache()
    var prevG : Graph[Vattr,Double] = null
    var i : Int = 0


    while (i < 10) {
      var messages = g.mapReduceTriplets(sendMessage, mergeMsgs)
      var activeMessages = messages.count()
      if (dbg) {
        println("Stage: " + i.toString)
        println(messages.collect().mkString("\n"))
      }

      // receive messages. At this point, I am receiving messages from stage i.
      var newVerts = g.vertices.innerJoin(messages)(vertexProgram).cache()

      // update graph with new vertices
      prevG  = g

      // after this point, the vertex is in stage 1.
      g = g.outerJoinVertices(newVerts) { (vid, oldAttr, newAttr) =>
        val attr = newAttr.getOrElse(oldAttr)
        Vattr(attr.disturbed,
          attr.affected,
          attr.globalStage + 1, 
          attr.vertexStage,
          attr.participate,
          attr.distSoFar,
          attr.memo
          )
      }
      g.cache()

      val oldMessages = messages

      //debug
      //if (dbg) 
      //  g.vertices.saveAsTextFile("/user/debug/run"+i)
      // next round of messages
      //messages = g.mapReduceTriplets(sendMessage, mergeMsgs) // check Some(...)
      //activeMessages = messages.count()

      // debug
      //debug
      //messages.saveAsTextFile(outFile)


      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
      // count the iteration
     

      i += 1


    } //while 
    //saveDists(g)
      return g
  }
}
