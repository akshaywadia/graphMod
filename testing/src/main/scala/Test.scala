import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger 
import org.apache.log4j.Level 
import scala.util.Random



object Test {
  def main(args : Array[String]) {
    // logging off
    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF) 
    val rand = new Random()
    // Create SparkContext -- boilerplate
    val conf = new SparkConf().setMaster("local[4]").setAppName("Test")
    val sc = new SparkContext(conf)

    //val file = sc.textFile("/user/akshay/z1.gr")
    val file = sc.textFile("/user/akshay/tree1024.gr")
    val edrdd = file.map{ ed =>
      val comps = ed.split(" ")
      new Edge(comps(0).toInt, comps(1).toInt, comps(2).toDouble) }
      var gr = Graph.fromEdges(edrdd, 0)

      val gmod = new graphMod()
      var postrun = gmod.run(gmod.initVattr(gr))
      println("First run over ...")
      //val edRdd = sc.textFile("/user/ed.txt")
      //var g = gmod.updateEdgeBatch(edRdd, postrun)


      //g.edges.saveAsTextFile("/user/akshay/delta/batch")
      var i : Int = 0
      while (i < 10) {
      println("Stage " + i.toString + " Starting reset")
      var modgr = gmod.resetGraph(postrun)
      
      //var count = modgr.vertices.map{ case (vid,vattr) => vattr.disturbed}.reduce((a,b) => a+b)
      //println("first run count: " + count.toString)
      
      // pick random edge, by selecting a random vertex, and a random
      // bit for left/right child
      var src = rand.nextInt(510) + 1
      var dst = if (rand.nextBoolean()) (2*src) else (2*src + 1)
      var wt = 2*i
      var ed = src.toString + " " + dst.toString + " " + wt.toDouble

      var gup = gmod.updateEdge(ed, modgr)
      println("Starting second run...")
      postrun = gmod.run(gup)
      println("Second run over...")
      i += 1
      }
      println("\n")
//      var deltav = postup.vertices.filter{ case (vid,vattr) => vattr.disturbed != 0}
 //     println("Post count: " + deltav.count())  
//      postup.vertices.saveAsTextFile("/user/akshay/delta/v")
 //     postup.edges.saveAsTextFile("/user/akshay/delta/e")
  //    deltav.saveAsTextFile("/user/akshay/delta/dv")

/////////// working debug /////////////////
/*      val gmod = new graphMod()

      val modifiedGraph = gmod.initVattr(gr)

      val initialrun = gmod.run(modifiedGraph)

      val rgraph = gmod.resetGraph(initialrun)
      val egraph = gmod.updateEdge("0 2 1.0", rgraph)
//      egraph.edges.saveAsTextFile("/user/akshay/delta/e")
 //     egraph.vertices.saveAsTextFile("/user/akshay/delta/v")

      val postgraph = gmod.run(egraph,true)
      postgraph.vertices.saveAsTextFile("/user/akshay/delta/v")
      val count = postgraph.vertices.map{ case (vid,vattr) => vattr.disturbed}.reduce((a,b) => a+b)
      println("Post count: " + count)  */
/////////// working debug /////////////////





//      initialrun.vertices.saveAsTextFile("/user/akshay/delta/final")
 //     val count = initialrun.vertices.map{ case (vid,vattr) => vattr.disturbed}.reduce((a,b) => a+b)
  //    println("Pre update: " + count.toString)
  //    val modGr2 = gmod.updateEdge("1 3 1.0", initialrun)
//      modGr2.vertices.saveAsTextFile("/user/akshay/delta/verEd")
//      modGr2.edges.saveAsTextFile("/user/akshay/delta/edEd")


      // -- for debug only --
      // reset vertex disturbances
   //  val modgr3 = gmod.resetGraph(modGr2)
 //    modgr3.vertices.saveAsTextFile("/user/akshay/delta/postver")
 //    modgr3.edges.saveAsTextFile("/user/akshay/delta/posted")

/*      val finalgr = gmod.run(modgr3,true)
      
      finalgr.vertices.saveAsTextFile("/user/akshay/delta/final")
        val count2 = finalgr.vertices.map{ case (vid,vattr) => vattr.disturbed}.reduce((a,b) => a+b)
      println("Post update: " + count.toString) */

  }
}
