package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment6;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._


object ApplyEnsembleSpamClassifier  {

  val log = Logger.getLogger(getClass().getName())

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, model, method, output)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model", required = true)
    val method = opt[String](descr = "method", required = true)
    val output = opt[String](descr = "output", required = true)
  }

  def main(argv: Array[String]) {

    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())
    log.info("Output: " + args.output())

    val model = args.model()
    val method = args.method()
    val input = args.input()
    val output = args.output()


    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val weight_map_x = sc.broadcast(sc.textFile(s"${model}/part-00000").map(r => {
      val sp = r.replaceAll("\\(", "").replaceAll("\\)","").trim.split (",",-1)
      (sp(0).toInt, sp(1).toDouble)
    }).collect.toMap)

    val weight_map_y = sc.broadcast(sc.textFile(s"${model}/part-00001").map(r => {
      val sp = r.replaceAll("\\(", "").replaceAll("\\)","").trim.split (",",-1)
      (sp(0).toInt, sp(1).toDouble)
    }).collect.toMap)

    val weight_map_britney = sc.broadcast(sc.textFile(s"${model}/part-00002").map(r => {
      val sp = r.replaceAll("\\(", "").replaceAll("\\)","").trim.split (",",-1)
      (sp(0).toInt, sp(1).toDouble)
    }).collect.toMap)

    val textFile = sc.textFile(input)

    val delta = 0.002

    val apply = textFile.map(line => {
      val sp = line.split("\\s+", -1)
      val docid = sp(0)
      var isSpam = 0
      if (sp(1) == "spam") {
        isSpam = 1
      }

      var numSpam = 0

      var score = 0d
      var scores = (0d,0d,0d)


      sp.slice(2, sp.length).map(_.toInt).foreach(f => {
          if (method == "average") {
            score += weight_map_x.value.getOrElse(f, 0d) + weight_map_y.value.getOrElse(f, 0d) + weight_map_britney.value.getOrElse(f, 0d)
          }
          else if (method == "vote") {
            scores = (scores._1 + weight_map_x.value.getOrElse(f, 0d), scores._2 + weight_map_y.value.getOrElse(f, 0d), scores._3 + weight_map_britney.value.getOrElse(f, 0d))
          }
      })


      if (method == "average") {
        score = score/3

        if (score > 0) {
          (docid, sp(1), score, "spam")
        }
        else {
          (docid, sp(1), score, "ham")
        }
      }
      else if (method == "vote") {
        if (scores._1 > 0) { numSpam += 1 }
        if (scores._2 > 0) { numSpam += 1 }
        if (scores._3 > 0) { numSpam += 1 }
        val numHam = (3 - numSpam)
        score = numSpam - (3 - numSpam)
        if (numSpam > numHam) {
          (docid, sp(1), score, "spam")
        }
        else {
          (docid, sp(1), score, "ham")
        }
      }
    })
      .saveAsTextFile(output)
  }

}
