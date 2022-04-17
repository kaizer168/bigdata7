package spark.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.FileUtil
import scala.collection.mutable.ArrayBuffer
import scopt.OParser

object MyDistCP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MyDistCP")
      .getOrCreate()

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("MyDistCP"),
        head("MyDistCP", "1.x"),
        opt[Int]('m', "max")
          .action((x, c) => c.copy(maxConcurrentTask = x))
          .text("max concurrence"),
        opt[Unit]('i', "ignoreFailure")
          .action((_, c) => c.copy(ignoreFailures = true))
          .text("ignore failures"),
        help("help").text("prints this usage text"),
        arg[File]("<Source> <Target>")
          .unbounded()
          .optional()
          .action((x, c) => c.copy(source = x, target = x))
          .text("Source Target")
      )
    }

    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        checkDir(spark, source, target, fileList, options)
        Copy(spark, fileList, options)
      case _ =>
      // arguments are bad, error message will have been displayed
    }

    def checkDir(sparkSession: SparkSession, sourcePath: Path, targetPath: Path,
                 fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      fs.listStatus(sourcePath)
        .foreach(currPath => {
          if (currPath.isDirectory) {
            val subPath = currPath.getPath.toString.split(sourcePath.toString)(1)
            val nextTargetPath = new Path(targetPath + subPath)
            try {
              fs.mkdirs(nextTargetPath)
            } catch {
              case ex: Exception => if (!options.ignoreFailures) throw ex else logWarning(ex.getMessage)
            }
            checkDir(sparkSession, currPath.getPath, nextTargetPath, fileList, options)
          } else {
            fileList.append((currPath.getPath, targetPath))
          }
        })
    }

    def Copy(sparkSession: SparkSession, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {
      val sc = sparkSession.sparkContext
      val maxConcurrentTask = Some(Options.maxConcurrentTask).getOrElse(5)
      val rdd = sc.makeRDD(fileList, maxConcurrentTask)

      rdd.mapPartitions(ite => {
        val hadoopConf = new Configuration()
        ite.foreach(tup => {
          try {
            FileUtil.copy(tup._1.getFileSystem(hadoopConf), tup._1, tup._2.getFileSystem(hadoopConf)
              tup._2
            , false
            , hadoopConf
            )
          } catch {
            case ex: Exception => if (!options.ignoreFailures) throw ex else logWarning(ex.getMessage)
          }
        })
        ite
      }).collect()
    }
  }
}
// Incomplete
