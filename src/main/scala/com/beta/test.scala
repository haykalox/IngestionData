package com.beta

import com.typesafe.config.ConfigFactory

object test {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("application.conf").getConfig("tables")
    val data = config.getString("tables")

    println(data)


  }
}
