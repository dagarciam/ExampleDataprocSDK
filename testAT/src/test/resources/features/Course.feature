Feature: Feature for AmazonReports

  Scenario: Test Engine should return 0 in success execution
    Given a config file with path: src/test/resources/config/CourseTestAT.conf
    When I execute the process: CourseSparkProcess
    Then the exit code should be 0

    Given a dataframe outputDF in path: src/test/resources/data/course/output/t_fdev_fifa22
    Then outputDF dataframe has exactly 352 records
