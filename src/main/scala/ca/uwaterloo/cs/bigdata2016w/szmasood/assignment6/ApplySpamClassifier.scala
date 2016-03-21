package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment6;

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._


object ApplySpamClassifier  {
  val log = Logger.getLogger(getClass().getName())

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = false)
    val model = opt[String](descr = "model", required = true)
  }

  def main(argv: Array[String]) {

    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val model = args.model()
    val output = args.output()
    val input = args.input()


    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(output)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val weight_map = sc.broadcast(sc.textFile(model).map(r => {
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

      var score = 0d
      sp.slice(2, sp.length).foreach(f => if (weight_map.value.contains(f.toInt)) score += weight_map.value(f.toInt))

      if (score > 0) {
        (docid, sp(1), score, "spam")
      }
      else {
        (docid, sp(1), score, "ham")
      }
    })
    .saveAsTextFile(output)
  }

}
