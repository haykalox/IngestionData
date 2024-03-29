package com.beta.Connector

import org.apache.spark.sql.SparkSession

class SparkConnector {
  def getSession() = {
    SparkSession
      .builder()
      .appName("Ingestion")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
  }
}

