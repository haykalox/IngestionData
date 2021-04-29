package com.beta

import com.beta.Connector.{JsonToCs, MapToJson, SparkConnector, Xls}
import com.beta.model.Sc

object ReadXls {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    import spark.implicits._

    val dx = new Xls
    val df = dx.readData
    val dr = df.rdd.map(r => new Sc(
      orderDate = r.getString(0),
      region = r.getString(1),
      rep = r.getString(2),
      item = r.getString(3),
      units = r.getString(4),
      unitsCost = r.getFloat(5),
      total = r.getFloat(6)
    )).toDF()

    df.show(false)
    dr.show(false)

    Map("OrderDate" -> "12/01/01", "Region" -> "central")
    """{"orderDate":"12/01/01","Region":"central"}"""

    val dz = df.rdd.map(r => {
      val rowAsMap: Map[String, Any] = r.getValuesMap[Any](r.schema.fieldNames) //
      val rowAsString: String = MapToJson.toJson(rowAsMap)
      val rowAsSc: Sc = JsonToCs.jsonToCs(rowAsString)
      rowAsSc
    }
    ).toDF()
dz.show()
    /*
    val dw = dx.writeData(dr)

    spark.sql("SELECT * FROM xls").show()
*/
  }

}
