
# Dataproc Spark Scala Job - CrashCourse

Project with material required for the Scala processing certification course, by Datio America.

## Getting started with this project
### First Compilation

Make a clean install in the root directory of the project

```bash
mvn clean install
```

### First Execution with the SparkLauncher of dataproc-sdk libraries

Go to your IDE run configurations window and set the following configuration:
 * Main class: `com.bbva.datioamproduct.fdevdatio.CrashCourseLauncher`
 * VM options: `-Dspark.master=local[*]`
 * Program arguments should be a valid path to a configuration file: `scalacrashcourse/src/main/resources/config/application.conf`
 * Working directory should point to the root path of the project: `/home/example/workspace/fdevdatio-scalacrashcourse`
 * Use classpath of the main implementation module => `scalacrashcourse`
 * Set the location of the local input and output files as environment variables:
   * INPUT_SCHEMA_PATH = "scalacrashcourse/src/main/resources/schema/inputSchema.json"
   * OUTPUT_SCHEMA_PATH = "scalacrashcourse/src/main/resources/schema/outputSchema.json"
   * INPUT_PATH = "scalacrashcourse/src/main/resources/data/olympics.csv"
   * OUTPUT_PATH = "scalacrashcourse/target/data/result"

You will also need to enable the maven profile `run-local`.

For newer versions of Intellij IDEA, make sure that you have selected both 'Add VM Options' and 'Include dependencies with "Provided" scope' features.

### Troubleshooting

* I got `java.lang.NoClassDefFoundError: org/slf4j/impl/StaticLoggerBinder` running this project in a local environment

Please add "logback" dependency in the parent POM file:
```xml
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>${logback.version}</version>
</dependency>
```
