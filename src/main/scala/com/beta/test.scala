package com.beta
import com.beta.Connector.{ConfToJson, SparkConnector}
import org.json4s.DefaultFormats


object test {
  def main(args: Array[String]): Unit = {
    case class Sc(nom1:String,Dtype1:String,nom2:String,Dtype2:String,nom3:String,Dtype3:String,nom4:String,Dtype4:String,nom5:String,Dtype5:String,nom6:String,Dtype6:String,nom7:String,Dtype7:String)
    val aa = new  ConfToJson("colonne").res

    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    val Sr = parse(aa).extract[Sc]

    println("-------------------------")
    println(aa)
    println("-------------------------")
    println(Sr)


    val Spark = new SparkConnector
    val spark = Spark.getSession()
    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS
         |test (${Sr.nom1}:${Sr.Dtype1},${Sr.nom2}:${Sr.Dtype2},${Sr.nom3}::${Sr.Dtype3},${Sr.nom4}:${Sr.Dtype4},${Sr.nom5}:${Sr.Dtype5},${Sr.nom6}:${Sr.Dtype6},${Sr.nom7}:${Sr.Dtype7})
         |ROW FORMAT DELIMITED
         |FIELDS TERMINATED BY ';'
         |STORED AS TEXTFILE
         |LOCATION '/apps/hive/external/default/test'
         |""".stripMargin)

    spark.sql("SELECT * FROM test").show()


  }
}
