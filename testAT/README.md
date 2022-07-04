# Acceptance Tests for Java Spark projects

An acceptance test is a formal description of the behavior of a software product, generally expressed as an example or 
a usage scenario. The AT acts as a verification of the required business function and proper functioning of the system, 
emulating real-world usage conditions on behalf of customers.

`testAT` module contains the source code and files for acceptance testing and defines the way other components are 
available for that purpose.

## testAT module components
 * Acceptance tests resource files: this directory contains files used as resources in acceptance tests, such as 
feature files, data, process configuration and extra files to be included in the Spark execution context.
 * Acceptance Test Runner: `RunCukesIT.java` file is the one that performs the test execution.
 * Custom steps: it is possible that a particular project might need to extend this and implement its own steps.

## Acceptance Tests resources
Resource files for acceptance tests should be included in `src/test/resources` relative path. Type of resources 
are defined in the following subfolders:
 * `features`: here we define our test files using Gherkin language.
 * `data`: contains the data to be used in our tests.
 * `config`: directory to store config files for our process.
 * `schema`: schemas used for testing.

## Feature files
It contains the definition of our acceptance tests in Gherkin language. For now, some considerations are needed to 
make it work:
* Tags: Cucumber tags (`@tag`) can be used in the feature file in order to organize and launch specific scenarios. 
* File paths: As long as we don't use Docker container for acceptance tests anymore, paths to files in the defined 
steps must be targeted to local (relative) paths (example: `src/test/resource/config/processExample.conf`).

## Common Steps

### Initialize process with an Id

```
Given The id of the process as CrashCourse
```

### Initialize environment variables

```
And The env 'INPUT_PATH' targeting the resource file 'data/olympics.csv'
And The env 'INPUT_SCHEMA_PATH' targeting the resource file 'schema/inputSchema.json'
And The env 'OUTPUT_PATH' targeting the target file 'output'
And The env 'OUTPUT_SCHEMA_PATH' targeting the resource file 'schema/outputSchema.json'
```

### Initialize process config

```
And A config file with the contents:
    """
    CrashCourse {
      inputSchemaPath = ${?INPUT_SCHEMA_PATH}
      outputSchemaPath = ${?OUTPUT_SCHEMA_PATH}
      inputPath = ${?INPUT_PATH}
      outputPath = ${?OUTPUT_PATH}
    }
    """
```

### Execute process

Runs main class of the process (runProcess method).

```
When Executing the Launcher
```

### Check the exit code of the process

Checks that the process ends correctly or not.

```
Then The exit code should be 0
```

# Useful links
* [Cucumber](https://cucumber.io/)
* [Gherkin](https://docs.cucumber.io/gherkin/reference/)
