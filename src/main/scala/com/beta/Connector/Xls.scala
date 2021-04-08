package com.beta.Connector

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class Xls {
  val Spark = new SparkConnector
  val spark = Spark.getSession()

  def readData(location: String): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Required
      .option("inferSchema", "false") // Optional, default: false
      .load(location)
  }

  def writeData(dr: DataFrame, locationD: String, tb: String): Unit = {

    val dx=dr.select(
      col("OrderDate"),
      col("Region"),
      col("Rep"),
      col("Item"),
      col("Units"),
      col("Unit Cost"),
      col("Total")
    )
    dx.write
      .format("csv")
      .mode("overwrite")
      .option("delimiter", ";")
      .save(locationD)


    spark.sql(s"""drop table if EXISTS $tb""")

    val config = ConfigFactory.load("application.conf").getConfig("tables")
    val sheman = config.getConfig("sheman")
    val data = sheman.getString("des")

    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS
         |$tb ($data)
         |ROW FORMAT DELIMITED
         |FIELDS TERMINATED BY ';'
         |STORED AS TEXTFILE
         |LOCATION '/apps/hive/external/default/$tb'
         |""".stripMargin)

  }
}