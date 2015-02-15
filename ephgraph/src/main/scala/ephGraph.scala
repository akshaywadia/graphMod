import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


class ephGraph extends java.io.Serializable {

  /****
   * TYPE DECLARATIONS for storing state information for each vertex.
   ****/

  /* This is the state storing data structure. For every (directed) adjacent vertex, 
   * this stores the weight of the shortest path flowing through that vertex.
   * NB: No need to remember the *stage* in which these messages were received.
   * NB: This MsgDigest type has two uses: (1) it is output by the reduce phase, 
   * (2) it is stored at a vertex as the memo. This can be confusing.
   */
  type MsgDigest= Map[VertexId,Double]

  /* As additional state, we will also remember the current shortest distance,
   * even though it can be calculated from the stored MsgDigest - just
   * to make things clear.
   */
  case class Memo(dist : Double, msgs : MsgDigest)

  /****
   * GRAPH UPDATE FUNCTIONS
   ****/

  /*
   * Updates edges for an existing graph.
   *        edge      :   String, with format "srcID dstId weight", which are Long,
   *                      Long, and Double
   *        gr        :   Graphp[Vattr,Double], the post first-run graph, i.e., 
   *                      post-init graph.
   *        *return*  :   new graph with updated edge weight.
   */
  
  def updateEdge(edge:String, gr:Graph[Memo,Double]) : Graph[Memo,Double] = {
    // parse edge into edge components
    val comps = edge.split(" ")
    val src = comps(0).toLong
    val dst = comps(1).toLong
    val attr = comps(2).toDouble

    // update weight
    val updatedEd = gr.mapEdges{ ed =>
      if (ed.srcId == src && ed.dstId == dst) attr else ed.attr
    }
    return updatedEd
  }

  /*
   * Batch udpate of edges.
   *        edges    :   RDD[String] of the form "src dest wt"
   *        gr       :   Post-first run graph
   *        *return* :   Updated graph
   */

  def updateEdgeBatch(edges:RDD[String], gr:Graph[Memo,Double]) : Graph[Memo,Double] = {
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

  /* Increment map. For testing and comparing with existing GraphX implementation.
   * Note that in my case, SPMap is just a single value, the first component
   * of Vattr. For landmarks, should be generalized to maps.*/
  private def incrementMap(dist : Double) : Double = dist + 1

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

  private def computeState(msgs : MsgDigest) : Double =
    msgs.values.reduce( (a,b) => math.min(a,b)) 

  /* vertexProgram. This is run only for vertices that recieve input messages.
   * Recall that this is executed
   * *after* the innerJoin in Pregel. The result of this join is (vid, Memo, msgDigest).
   * - Create new memo, which is mergeMsgs of incoming messages, and previous memo.
   * - Recompute state.
   */
  def vertexProgram(id : VertexId, memo : Memo, messages : MsgDigest) : Memo = {
    val newMsgs = mergeMsgs(memo.msgs, messages)
    val newDist = computeState(newMsgs)
    return Memo(newDist, newMsgs)
    }

  /** HELPER FUNCTIONS for sendMessage **/

  /* sendMsg : looks at a triplet, and prepares message for the destination vertex.
   * - Create the potential message to be sent from src to dst (i.e., src.memo.dist + ed.wt)
   * - Check if this message will change the memoized msg at dst. If yes, then send msg, 
   *   else, not.
   */
  def sendMessage(edge: EdgeTriplet[Memo,Double]) : Iterator[(VertexId, MsgDigest)] = {
    val potentialMsg = edge.srcAttr.dist + edge.attr
    if (edge.dstAttr.msgs.contains(edge.srcId) && (edge.dstAttr.msgs(edge.srcId) == potentialMsg))
      return Iterator.empty
    else
      return Iterator((edge.dstId, Map(edge.srcId -> potentialMsg)))
  }


  /* Initialize vertices and convert Graph[Int,Double] to Graph[Memo,Double]
   */
  def initAttr(gr : Graph[Int,Double]) : Graph[Memo,Double] =   
    return gr.mapVertices( (vid,attr) => Memo(Double.MaxValue, Map[VertexId, Double]()) )

  def run(gr : Graph[Memo,Double]) : Graph[Memo,Double] = {
    val initMsg = Map[VertexId, Double]()
    Pregel(gr,initMsg)(vertexProgram, sendMessage, mergeMsgs)
  }

  def saveToText(path : String, gr : Graph[Memo,Double]) : Unit = 
    gr.vertices.map{ case (vid,attr) => (vid, attr.dist) }.saveAsTextFile(path)
}
