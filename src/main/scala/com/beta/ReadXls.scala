package com.beta

import com.beta.Connector.{SparkConnector, Xls}

object ReadXls {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Xls
    val dr = dx.readData

    val dw = dx.writeData(dr)

    spark.sql("SELECT * FROM xls").show()

  }

}
