package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment5;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.ListBuffer

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = false)
}

object Q1  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val dt = args.date()

    val conf = new SparkConf().setAppName("q1")
    val sc = new SparkContext(conf)

    val lineItems = s"${args.input()}/lineitem.tbl"

    val cnt = sc.textFile(lineItems)
      .filter (r => r.split("\\|",12)(10).contains(dt))
      .count

    System.out.println (s"ANSWER=${cnt}")
  }

}
