package org.apache.spark.Streaming.learning

object fileStream {

  def main(args: Array[String]): Unit = {
    import org.apache.spark._
    import org.apache.spark.streaming._

    val conf = new SparkConf().setAppName("StreamingContextTest").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(1))
//    ssc.fileStream[KeyClass, ValueClass, InputFormatClass](dataDirectory)
  }

}