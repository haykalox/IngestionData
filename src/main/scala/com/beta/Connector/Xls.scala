package com.beta.Connector

import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats

class Xls {
  val Spark = new SparkConnector
  val spark = Spark.getSession()

  case class SparkR(format: String, options:Map[String,String], location:String)
  case class SparkW(format:String,mode:String,options:Map[String,String],location:String,TbName:String)
  case class Sc(nom1:String,Dtype1:String,nom2:String,Dtype2:String,nom3:String,Dtype3:String,nom4:String,Dtype4:String,nom5:String,Dtype5:String,nom6:String,Dtype6:String,nom7:String,Dtype7:String)

  def readData: DataFrame = {
    val res = new  ConfToJson("read").res

    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    val Sr = parse(res).extract[SparkR]

    spark.read
      .format(Sr.format)
      .options(Sr.options)
      .load(Sr.location)
  }

  def writeData(dr: DataFrame): Unit = {
    import org.apache.spark.sql.functions.col
    val dx=dr.select(
      col("OrderDate"),
      col("Region"),
      col("Rep"),
      col("Item"),
      col("Units"),
      col("Unit Cost"),
      col("Total")
    )

    val res = new  ConfToJson("write").res

    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    val Sw = parse(res).extract[SparkW]

    dx.write
      .format(Sw.format)
      .mode(Sw.mode)
      .options(Sw.options)
      .save(Sw.location)


    spark.sql(s"""drop table if EXISTS ${Sw.TbName}""")

    val resSc = new  ConfToJson("colonne").res

    val Sr = parse(resSc).extract[Sc]

    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS
         |${Sw.TbName} (${Sr.nom1} ${Sr.Dtype1},${Sr.nom2} ${Sr.Dtype2},${Sr.nom3} ${Sr.Dtype3},${Sr.nom4} ${Sr.Dtype4},${Sr.nom5} ${Sr.Dtype5},${Sr.nom6} ${Sr.Dtype6},${Sr.nom7} ${Sr.Dtype7})
         |ROW FORMAT DELIMITED
         |FIELDS TERMINATED BY ';'
         |STORED AS TEXTFILE
         |LOCATION '/apps/hive/external/default/${Sw.TbName}'
         |""".stripMargin)

  }
}