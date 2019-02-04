# aggregation-state-udf

[![CircleCI](https://circleci.com/gh/gom/aggregation-state-udf.svg?style=svg)](https://circleci.com/gh/gom/aggregation-state-udf)

Hive and Presto UDF for storing and merging aggregation state.

## Function list
- approx_distinct_state
  - Approximate Count Distinct aggregation.
  - Returns aggregation states as binary.
- approx_distinct_merge
  - Merge the states and returns cardinality.

## Build
`$ ./gradlew clean shadow`
