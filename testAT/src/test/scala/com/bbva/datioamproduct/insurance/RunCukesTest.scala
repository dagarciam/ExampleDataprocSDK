package com.bbva.datioamproduct.insurance

import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("src/test/resources/features/insurance.feature"),
  glue = Array("com.bbva.datioamproduct.insurance.steps"),
  strict = true,
  plugin = Array("pretty"))
class RunCukesTest
