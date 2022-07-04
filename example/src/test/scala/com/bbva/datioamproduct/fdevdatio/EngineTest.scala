package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}

class EngineTest extends ContextProvider {

  "runProcess method" should "return 0 in success exit" in {

    val runtimeContext = new FakeRuntimeContext(config)
    val exitCode = new Engine().runProcess(runtimeContext)

    exitCode shouldBe 0
  }

}



