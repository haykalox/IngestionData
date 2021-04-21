package com.beta

import com.beta.Connector.{SparkConnector, Xls}

object ReadXls {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Xls
    val dr = dx.readData
dr.show()

    /*
    val dw = dx.writeData(dr,"/apps/hive/external/default/xls/","xls")

    spark.sql("SELECT * FROM xls").show()
 */
  }

}
