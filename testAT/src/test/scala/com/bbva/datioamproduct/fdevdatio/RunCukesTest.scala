package com.bbva.datioamproduct.fdevdatio

import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("src/test/resources/features/Example.feature"),
  glue = Array("com.bbva.datioamproduct.fdevdatio.steps"),
  strict = true,
  plugin = Array("pretty"))
class RunCukesTest
