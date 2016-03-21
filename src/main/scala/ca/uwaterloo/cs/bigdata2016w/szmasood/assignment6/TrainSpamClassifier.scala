package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment6;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._



object TrainSpamClassifier  {

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, model,shuffle)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model", required = true)
    val shuffle = opt[Boolean] (descr = "shuffle", required = false, default = Some(false))
  }

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val model = args.model()
    val input = args.input()
    val shuffle = args.shuffle()

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(input)
    val delta = 0.002

    val trained = textFile.map(line => {
        val sp = line.split("\\s+", -1)
        val docid = sp(0)
        var isSpam = 0
        if (sp(1) == "spam") {
          isSpam = 1
        }
        var key = 0d
        if (shuffle) {
          key = Math.random()
        }
        (key, (docid, isSpam, sp.slice(2, sp.length).map(_.toInt)))
      })


    val grouped = {
      if (shuffle) {
        trained.sortByKey()
          .map(r => (0d, (r._2._1, r._2._2, r._2._3)))
      }
      else {
        trained
      }
    }

      grouped.groupByKey(1)
      .flatMap (r => {
        val w = scala.collection.mutable.Map[Int, Double]()
        for (line <- r._2) {
          val isSpam = line._2
          var score = 0d
          line._3.foreach(f => if (w.contains(f)) score += w(f))
          val prob = 1.0 / (1 + Math.exp(-score))
          line._3.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        }
        w.toList
      })
      .saveAsTextFile(model)
  }

}
