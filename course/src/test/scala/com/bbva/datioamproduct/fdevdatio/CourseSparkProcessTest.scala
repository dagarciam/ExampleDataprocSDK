package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}

class CourseSparkProcessTest extends ContextProvider {

  "when I execute runProcess method with a correct RuntimeContext" should "return 0" in {

    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(config)
    val courseSparkProcess: CourseSparkProcess = new CourseSparkProcess()

    val exitCode: Int = courseSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe 0

  }

  it should "return exit code -1 when JoinException is 'arrojado" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(configJoinException)
    val courseSparkProcess: CourseSparkProcess = new CourseSparkProcess()

    val exitCode: Int = courseSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe -1
  }


}
