# DataFramesMeta v0.6 Release Notes

* The order of rows after a `@transform(gd::GroupedDataFrame, args...)` now matches the 
order of rows returned after `DataFrames.transform(gd::GroupedDataFrame, args...)`. 
`@select` now supports `GroupedDataFrames` ([#180])