Feature: Feature for AmazonReports

  Scenario: Test Engine should return 0 in success execution
    Given a config file with path: src/test/resources/config/application-testAT.conf
    When I execute the process: Engine
    Then the exit code should be 0

  Scenario: Validate the output has not values CH, IT, CZ or DK in country_code column
    Given a dataframe outputDF in path: src/test/resources/data/output/t_fdev_customersphones
    When I filter outputDF dataframe with the filter: country_code IN ("CH", "IT", "CZ", "DK") and save it as outputFilteredDF dataframe
    Then outputFilteredDF dataframe has exactly 0 records

  Scenario: Validate the output has just values MX, CA, BR, US, PE, JP, NZ, CO and AR in country_code
    When I filter outputDF dataframe with the filter: country_code IN ("MX", "CA", "BR", "US", "PE", "JP", "NZ", "CO", "AR") and save it as outputFilteredDF dataframe
    Then outputFilteredDF has exactly the same number of records than outputDF