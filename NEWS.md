# DataFramesMeta v0.6 Release Notes

* The order of rows after a `@transform(gd::GroupedDataFrame, args...)` now matches the 
  order of rows returned after `DataFrames.transform(gd::GroupedDataFrame, args...)`. 
* `@select` now supports `GroupedDataFrame` with the same behavior as 
  `DataFrames.select(df::GroupedDataFrame, args...)` ([#180])
* `@orderby(gd::GroupedDataFrame, args...)` now returns a `DataFrame` instead 
  of a `GroupedDataFrame`. Operations are performed by group similar to `@transform` 
  and `@select`. 