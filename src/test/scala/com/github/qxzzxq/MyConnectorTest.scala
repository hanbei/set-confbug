package com.github.qxzzxq

import io.github.setl.Setl
import io.github.setl.config.ConfigLoader
import org.scalatest.FunSuite
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}

class MyConnectorTest extends FunSuite {

  private var setl: Setl = _

  test("Connector reads array configuration") {
    val configLoader = ConfigLoader.builder()
      .setAppEnv("test")
      .setProperty("setl.config.spark.master", "local")
      .getOrCreate()
    val setl: Setl = Setl.builder()
      .setConfigLoader(configLoader)
      .getOrCreate()

    setl.setConnector("MyConnector", "MyConnector")

    val configuredConnector = setl.getConnector[MyConnector]("MyConnector")

    configuredConnector.name should be(Array("path1", "path2"))

  }

}
