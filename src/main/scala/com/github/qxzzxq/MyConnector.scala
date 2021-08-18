package com.github.qxzzxq

import io.github.setl.config.Conf
import io.github.setl.storage.connector.ConnectorInterface
import org.apache.spark.sql.DataFrame

class MyConnector extends ConnectorInterface {

  import spark.implicits._

  var _name: Array[String] = Array[String]()

  override def setConf(conf: Conf): Unit = {
    this._name = conf.getAs[Array[String]]("name", Array[String]())
  }

  override def read(): DataFrame = Seq(_name).toDF("MyConnector")

  override def write(t: DataFrame, suffix: Option[String]): Unit = log.debug("write with suffix")

  override def write(t: DataFrame): Unit = log.debug("write")

  def name: Array[String] = this._name

}
