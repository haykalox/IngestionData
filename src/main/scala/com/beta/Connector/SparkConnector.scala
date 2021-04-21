package com.beta.Connector

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats

class SparkConnector {
  case class SparkS(AppName: String, config:String, location:String)
  val config = ConfigFactory.load("application.conf")
  val app =config.getObject("SparkConnector").toString()
  val app1 = app.replace("SimpleConfigObject(","")
  val app2 = app1.replace(")","")

  import org.json4s.native.JsonMethods._
  implicit val formats = DefaultFormats
  val Sp = parse(app2).extract[SparkS]

  def getSession() = {
    SparkSession
      .builder()
      .appName(Sp.AppName)
      .config(Sp.config, Sp.location)
      .enableHiveSupport()
      .getOrCreate()
  }
}

