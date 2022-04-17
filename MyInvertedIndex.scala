package spark.demo

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{SparkConf, SparkContext}

object MyInvertedIndex {
  def main(args: Array[String]): Unit = {
    val inputPath : String = args.apply(0);
    val outputPath : String = args.apply(1);
    val conf = new SparkConf()
    conf.setAppName("MyInvertedIndex")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val fc = classOf[TextInputFormat]
    val kc = classOf[LongWritable]
    val vc = classOf[Text]
    val text = sc.newAPIHadoopFile(inputPath, fc, kc, vc, sc.hadoopConfiguration)
    val linesWithFileNames = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
      val file = inputSplit.asInstanceOf[FileSplit]
      iterator.map(tup => (file.getPath.toString.split("/").last, tup._2))
    })

    val tempIndex = linesWithFileNames.flatMap {
      case (fileName, text) => text.toString.split("\r\n")
        .flatMap(line => line.split(" "))
        .map{word => (word, fileName)}
    }

    val invertedIndex = tempIndex.groupByKey()

    val group = invertedIndex.map {
      case (word, tup) =>
        val fileCountMap = scala.collection.mutable.HashMap[String, Int]()
        for (fileName <- tup) {
          val count = fileCountMap.getOrElseUpdate(fileName, 0) + 1
          fileCountMap.put(fileName, count)
        }
        (word, fileCountMap)
    }.sortByKey().map(word => s"${word._1}:${word._2}")
    group.repartition(1).saveAsTextFile(outputPath)
  }
}
