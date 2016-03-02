package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment5;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.ListBuffer



object Q2  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val dt = args.date()

    val conf = new SparkConf().setAppName("q2")
    val sc = new SparkContext(conf)

    val base = args.input()
    val lineItemUri = s"${base}/lineitem.tbl"
    val ordersUri = s"${base}/orders.tbl"

    val clerks = sc.textFile(ordersUri)
        .map (r => {
          val sp = r.split("\\|",-1)
          (sp(0), sp(6))                //(orderkey, customer)
        })
      .cache()

    val lineOrderKeys = sc.textFile(lineItemUri)
      .filter (r => r.split("\\|",12)(10).contains(dt))
      .map (r => {
        val sp = r.split("\\|",12)
        (sp(0),1)
      })
      .cogroup(clerks)
      .filter(r => r._2._1.size != 0)
      .map(r=> {
          (r._2._2.head, r._1.toLong)
      })


    lineOrderKeys.takeOrdered(20)(Ordering[Long].on(x=>x._2)).foreach(println)
  }

}
