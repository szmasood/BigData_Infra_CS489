package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment5;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._



object Q3  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val dt = args.date()

    val conf = new SparkConf().setAppName("q3")
    val sc = new SparkContext(conf)

    val base = args.input()
    val lineItemUri = s"${base}/lineitem.tbl"
    val partsUri = s"${base}/part.tbl"
    val supplierUri = s"${base}/supplier.tbl"

    val parts = sc.broadcast(sc.textFile(partsUri).map(r => {
      val sp = r.split ("\\|",3)
      (sp(0), sp(1))
      }).collect.toMap)

    val suppliers = sc.broadcast(sc.textFile(supplierUri).map(r => {
      val sp = r.split ("\\|",3)
      (sp(0), sp(1))
    }).collect.toMap)


    val lineOrderKeys = sc.textFile(lineItemUri)
      .filter (r => r.split("\\|",12)(10).contains(dt))
      .map (r => {
        val sp = r.split("\\|",4)
        val l_partkey = sp(1)
        val l_suppkey = sp(2)
        val l_orderkey = sp(0)
        if (parts.value.contains(l_partkey) && suppliers.value.contains(l_suppkey)) {
          (l_orderkey.toLong, parts.value(l_partkey), suppliers.value(l_suppkey))
        }
        else {
          (Long.MinValue,"**-1","**-1")
        }
      })
      .filter(r=> r._1 != Long.MinValue)


    lineOrderKeys.takeOrdered(20)(Ordering[Long].on(x=>x._1)).foreach(println)
  }

}
