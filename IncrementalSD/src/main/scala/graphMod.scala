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

  /* Messge digest. Stores all incoming messages, indexed by VertexId, for a particular stage.*/
  type MsgDigest = Map[VertexId,Double]

  /* The memoization mechanism. Maps stage number to message digest received in that 
   * stage.  */
  case class MemoInfo(memoDist : Double, memoMessages : MsgDigest)
  type Memo = Map[Int,MemoInfo]

  /* Vertex attribute type -- 
   *      affected    :   whether graph update affects this vertex or not
   *      globalStage :   global stage number
   *      vertexStage :   the last stage vertex was active in
   *      participate :   will this vertex send message in this stage (used to signal 
   *                      map-reduce job)
   *      distSoFar   :   distance so far
   */
  case class Vattr(
    affected : Boolean,
    globalStage : Int,
    vertexStage : Int,
    participate : Boolean,
    distSoFar : Double,
    memo : Memo )


  /****
   * GRAPH UPDATE FUNCTIONS
   ****/
  
  /* Updates edges for an existing graph.
   *      edge      :   String, with format "srcID dstId weight", which are Long,
   *                    Long, and Double
   *      gr        :   Graph[Int,Double]
   *      *return*  :   new graph with updated edge weight.
   */
  def updateEdge(edge:String, gr:Graph[Vattr,Double]) : Graph[Vattr,Double] = {
    // parse edge into edge components
    val comps = edge.split(" ")
    val src = comps(0).toInt
    val dst = comps(1).toInt
    val attr = comps(2).toDouble

    // update weight
    val updatedEd = gr.mapEdges{ ed =>
      if (ed.srcId == src && ed.dstId == dst) attr else ed.attr
    }

    // update affectedness
    if (src == 0)      // root
      return updatedEd.mapVertices{ case (vid,vattr) =>
        val newAffected = if (vid == 0) true else vattr.participate 
        Vattr(vattr.disturbed, 
          true,
          vattr.globalStage,
          vattr.vertexStage,
          vattr.participate,
          vattr.distSoFar,

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


  /* Increment map. Only for experimentation.
   * Note that in my case, messages received are scalars. 
   * For LANDMARKS, should be generalized to maps.
   */
  private def incrementMap(dist : Double) : Double = dist + 1

  /* This is the 'reduce' function. A vertex receives messages from all 
   * neighbours, and the reduce function reduces all incoming messages to 
   * one message. Here, it is simply the min function. For landmarks, it will
   * be as in graphx docs. 
   */
  private def addMaps(dist1 : Double, dist2 : Double) : Double = math.min(dist1,dist2)

  
  /* Functions used for Pregel interface. */

  /* This is the reduce function in the map-reduce over triplets. Simply concatenates all the 
   * messages directed towards a particular vertex. Note that we need to store all (vid,msg) 
   * pairs as they need to be memoized -- cf. vertexProgram.
   *
   * Reminder: Don't need to send memoized state, that is only local to 
   * each vertex.
   */
  private def mergeMsgs(memoMsg : MsgDigest, newMsg : MsgDigest) : MsgDigest = 
    memoMsg ++ newMsg



  /* Computes the current state (which is the current shortest distance). 
   * In NORMAL (non-incremental, first pass) operation, recieves all messages,
   * and simply computes the min.
   * In INCREMENTAL operation, receives all messages, merges them with previously
   * memoized messages, which are also part of the computation, and then computes the min.
   *      attr      :     Current vertex attr. Needed to recall previous messages.
   *      messages  :     New messages received in this stage, which is Map[vid,double]
   *      *return*  :     New shortest distance.
   */
  private def computeState(attr : Vattr, messages : MsgDigest) : Double = {
    // get memoized messages for this stage, if they exist
    val memoMsgs = if (attr.memo contains attr.globalStage) 
                        attr.memo(attr.globalStage).memoMessages 
                    else Map[Long,Double]()

    val mergedMessages = mergeMsgs(memoMsgs, messages)

    // compute min
    return (attr.distSoFar :: mergedMessages.values.toList).reduce((a,b) => math.min(a,b))
  }



  /* Compute whether current vertex participates in this stage or not. For complete description
   * of this function, see Section 3.2 of the above paper. Note that, it is possible for a 
   * vertex to participate, even if it did not receive any messages in this stage. This happens
   * in incremental operation for affected vertex. This case is handled in the sendMsgs
   * function.
   */
  private def participate(attr : Vattr, messages : MsgDigest) : Boolean = {
    val memo = attr.memo

    // If vertex receives message in this stage, but there is no memoized record, participate.
    if (!(memo contains attr.globalStage)) return true
    else {
      val memoInfo = memo(attr.globalStage)
      val memoDist = memoInfo.memoDist
      val memoMsgs = memoInfo.memoMessages
      val currentDist = computeState(attr, messages)

      // At least one different message was received.
      if (memoMsgs != messages) return true

      // Current state is different. 
      if (currentDist != memoDist) return true

      // Current vertex is affected.
      if (attr.affected) return true
      else return false
      
    }
  }

  /* vertexProgram. This is run for only the vertices that receive messages. 
   * Recall that this is executed *after* the innerJoin in Pregel. 
   * The result of this join is (vid, Vattr, msgDigest).
   * 1. set participation bit
   * 2. update vertex stage
   * 3. merge messages with memoized messages
   * 4. update current shortest distance
   * 5. memoize current state */
  def vertexProgram(id : VertexId, attr : Vattr, messages : MsgDigest) : Vattr = {
    // determine participation, for sendMsgs function
    val newParticipate = participate(attr,messages)

    // update vertex stage, again for the benefit of sendMsgs function
    val newVertexStage = (attr.globalStage+1)

    // get previously stored messages for this stage, and merge with current messages
    val memoizedMsgDigest  = if (attr.memo contains attr.globalStage) attr.memo(attr.globalStage).memoMessages else Map[Long,Double]()
    val mergedMessages = mergeMsgs(memoizedMsgDigest,messages) // returns map with updated messages

    // update current state, that is, shortest distance
    val newDist = computeState(attr, messages)

    // update memo for current stage
    val newMemo = attr.memo + (attr.globalStage -> MemoInfo(
      newDist,
      mergedMessages))

    // prepare and return new vertex attribute object with updated state
    return Vattr(
      attr.affected, 
      attr.globalStage, 
      newVertexStage, 
      newParticipate, 
      newDist,
      newMemo ) 
    }


  /* HELPER FUNCTIONS for sendMsgs */
  
  /* Check if participation status is current or not. This simply involves testing if
   * vertexStage matches globalStage or not. No match means that the vertex was NOT active
   * during this stage (vertexProgram was not executed in this stage)
   */
  private def isParticipateCurrent(attr : Vattr) : Boolean = attr.globalStage == attr.vertexStage

  /* Check is this vertex was active in the current stage, in normal operation. This involves
   * if there is any memo for this stage.
   */
  private def activeCurrentStage(attr : Vattr) : Boolean = attr.memo contains attr.globalStage


  /* Looks at each edge, and prepares message from src to dst.
   */
  def sendMessage(edge: EdgeTriplet[Vattr,Double]) : Iterator[(VertexId, MsgDigest)] = {
    /* If participation status current, then follow that. Else, check if this vertex is 
     * affected *and* expecting a message. In this case, send message. */
    if (isParticipateCurrent(edge.srcAttr)) {
      // check for receiver state optimization.
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

  /* Debug function */
  private def saveDists(gr : Graph[Vattr,Double]) : Unit = {
    gr.vertices.map{ case (vid,vattr) => (vid,vattr.distSoFar)}.saveAsTextFile("/user/debug/dists")
  }

  //def run()
  
  /* Pregel with stage numbers.
   */

  def run(graph: Graph[Int,Double], 
    dbg : Boolean = false,
    MaxIterations : Int = 10))
/*    activeDirection : EdgeDirection = EdgeDirectino.Either)
   (vertexProg : (VertexId, Vattr, MsgDigest) => Vattr,
     sendMsg : EdgeTriplet[Vattr,_] => Iterator[(VertexId,Vattr)],
     mergeMsg : (MsgDigest, MsgDigest) => MsgDigest) */
  : Graph[Vattr,Double] = {
    // prepare vertices
    val initVertexMsg = Vattr(true,0,0,false,Double.MaxValue,Map[Int,MemoInfo]())
    val initVertexMsgSource = Vattr(true,0,0,true,0.0,Map[Int,MemoInfo]())
    
    // serializable issue
    val setVertexAttr = (vid :VertexId, vdata : Int) => if (vid ==0) initVertexMsgSource else initVertexMsg
    var g = graph.mapVertices(setVertexAttr).cache()
//    var g = graph.mapVertices((vid, vdata) => if (vid == 0) initVertexMsgSource
//      else initVertexMsg).cache()

    // compute Stage 0 messages.
    //var messages = g.mapReduceTriplets(sendMessage, mergeMsgs)
    //var activeMessages = messages.count()


    // main loop, decide when to stop -- when no new messages.
    var prevG : Graph[Vattr,Double] = null
    var i : Int = 0


    while (i < MaxIterations) {
      var messages = g.mapReduceTriplets(sendMessage, mergeMsgs)
      var activeMessages = messages.count()

      //debug
      //messages.saveAsTextFile("/user/debug/init" + i)

      // receive messages. At this point, I am receiving messages from stage i.
      var newVerts = g.vertices.innerJoin(messages)(vertexProgram).cache()

      // update graph with new vertices
      prevG  = g
      // after this point, the vertex is in stage 1.
      g = g.outerJoinVertices(newVerts) { (vid, oldAttr, newAttr) =>
        val attr = newAttr.getOrElse(oldAttr)
        Vattr(attr.affected,
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
      if (dbg) 
        g.vertices.saveAsTextFile("/user/debug/run"+i)
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

  def getDists(gr : Graph[Vattr,Double]) : Unit = gr.vertices.map{ case (vid,vattr) => (vid,vattr.distSoFar)}.collect().mkString("\n")
}
