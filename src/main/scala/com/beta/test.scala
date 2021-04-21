package com.beta
import com.beta.Connector.SparkConnector


object test {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Info")
      .option("header", "true")
      .option("inferSchema","false")
      .load("/data/xls/ir211wk12sample.xls")
      .show()
  }
}
