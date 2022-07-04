package com.bbva.datioamproduct.fdevdatio.testUtils

import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config

class FakeRuntimeContext(config: Config) extends RuntimeContext {

  override def getProcessId: String = "fake"

  override def getConfig: Config = config

  override def getProperty(propertyName: String): AnyRef = {
    throw new UnsupportedOperationException
  }

  override def setAdditionalInfo(additionalInfo: String): Unit = {}

  override def setMessage(message: String): Unit = {}

  override def setUserCode(userCode: String): Unit = {}
}
