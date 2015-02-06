import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object Test {
  def main(args : Array[String]) {
    // Create SparkContext -- boilerplate
    val conf = new SparkConf().setMaster("local[4]").setAppName("Test")
    val sc = new SparkContext(conf)

    val file = sc.textFile("/user/z.gr")
    val edrdd = file.map{ ed =>
      val comps = ed.split(" ")
      new Edge(comps(0).toInt, comps(1).toInt, comps(2).toDouble) }
      var gr = Graph.fromEdges(edrdd, 0)

      val gmod = new graphMod()

      val modifiedGraph = gmod.initVattr(gr)

      val initialrun = gmod.run(modifiedGraph)

//      initialrun.vertices.saveAsTextFile("/user/akshay/delta/final")
 //     val count = initialrun.vertices.map{ case (vid,vattr) => vattr.disturbed}.reduce((a,b) => a+b)
  //    println("Pre update: " + count.toString)
      val modGr2 = gmod.updateEdge("1 3 1.0", initialrun)
      modGr2.vertices.saveAsTextFile("/user/akshay/delta/verEd")
      modGr2.edges.saveAsTextFile("/user/akshay/delta/edEd")


      // -- for debug only --
      // reset vertex disturbances
     val modgr3 = gmod.resetGraph(modGr2)
     modgr3.vertices.saveAsTextFile("/user/akshay/delta/postver")
     modgr3.edges.saveAsTextFile("/user/akshay/delta/posted")

/*      val finalgr = gmod.run(modgr3,true)
      
      finalgr.vertices.saveAsTextFile("/user/akshay/delta/final")
        val count2 = finalgr.vertices.map{ case (vid,vattr) => vattr.disturbed}.reduce((a,b) => a+b)
      println("Post update: " + count.toString) */

  }
}
