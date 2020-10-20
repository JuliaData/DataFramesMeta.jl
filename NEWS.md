# DataFramesMeta v0.6 Release Notes

* The order of rows after a `@transform(gd::GroupedDataFrame, args...)` now matches the 
  order of rows returned after `DataFrames.transform(gd::GroupedDataFrame, args...)`. 
* `@select` now supports `GroupedDataFrame` with the same behavior as 
  `DataFrames.select(df::GroupedDataFrame, args...)` ([#180])
* `@orderby(gd::GroupedDataFrame, args...)` is now reserved and will error.
* Restrictions are imposed on the types of column references allowed when using `cols`. 
  Mixing integer column references with other types now errors. ([#183])
* `@where` with a grouped data frame will now perform operations by group and filter
  rows in the parent `DataFrame`. The operation no longer filters groups. Return a 
  fresh `DataFrame`.
