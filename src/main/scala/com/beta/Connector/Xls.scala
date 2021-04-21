package com.beta.Connector

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.json4s.DefaultFormats

import scala.collection.JavaConversions.mapAsScalaMap

class Xls {
  val Spark = new SparkConnector
  val spark = Spark.getSession()

  def readData: DataFrame = {
    case class SparkR(format: String, options:Map[String,String], location:String)
    val config = ConfigFactory.load("application.conf")
    val app =config.getObject("read").map({case (k, v) => (k, v.unwrapped())}).toMap
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val res =mapper.writeValueAsString(app)


    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    val Sr = parse(res).extract[SparkR]
    val testO=mapper.writeValueAsString(Sr.options)
    spark.read
      .format(s"${Sr.format}")
      .options(testO)
      .load(s"${Sr.location}")
  }

  def writeData(dr: DataFrame, locationD: String, tb: String): Unit = {
    case class coll(s1:String,s2:String,s3:String,s4:String,s5:String,s6:String,s7:String)
    case class Type(t1:String,t2:String,t3:String,t4:String,t5:String,t6:String,t7:String)

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

/** lecture des noms des colonne et leur type */
 /*   val config = ConfigFactory.load("application.conf").getConfig("tables")
    val col1 = config.getConfig("col1")
    val s1 = col1.getString("nom")
    val t1 =col1.getString("type")
    val col2 = config.getConfig("col2")
    val s2 = col2.getString("nom")
    val t2 =col2.getString("type")

    val col3 = config.getConfig("col3")
    val s3 = col3.getString("nom")
    val t3 =col3.getString("type")

    val col4 = config.getConfig("col4")
    val s4 = col4.getString("nom")
    val t4 =col4.getString("type")

    val col5 = config.getConfig("col5")
    val s5 = col5.getString("nom")
    val t5 =col5.getString("type")

    val col6 = config.getConfig("col6")
    val s6 = col6.getString("nom")
    val t6 =col6.getString("type")

    val col7 = config.getConfig("col7")
    val s7 = col7.getString("nom")
    val t7 =col7.getString("type")

    val schema=coll(s1,s2,s3,s4,s5,s6,s7)
    val dtype=Type(t1,t2,t3,t4,t5,t6,t7)

    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS
         |$tb (${schema.s1} ${dtype.t1},${schema.s2} ${dtype.t2},${schema.s3} ${dtype.t3},${schema.s4} ${dtype.t4},${schema.s5} ${dtype.t5},${schema.s6} ${dtype.t6},${schema.s7} ${dtype.t7})
         |ROW FORMAT DELIMITED
         |FIELDS TERMINATED BY ';'
         |STORED AS TEXTFILE
         |LOCATION '/apps/hive/external/default/$tb'
         |""".stripMargin)
*/
  }
}