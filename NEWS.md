# DataFramesMeta v0.6 Release Notes

* The order of rows after a `@transform(gd::GroupedDataFrame, args...)` now matches the 
  order of rows returned after `DataFrames.transform(gd::GroupedDataFrame, args...)`. 
* `@select` now supports `GroupedDataFrame` with the same behavior as 
  `DataFrames.select(df::GroupedDataFrame, args...)` ([#180])
* Restrictions are imposed on the types of column references allowed when using `cols`. 
  Mixing integer column references with other types now errors. ([#183])