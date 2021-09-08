# DataFramesMeta v0.9.1 Release notes

* Fix a bug with `@rtransform!` and `@rselect!`, where the macros were not actually mutating the data frame. ([#294](https://github.com/JuliaData/DataFramesMeta.jl/pull/294))

# DataFramesMeta v0.9.0 Release notes

* Add `@passmissing` flag for skipping missing values in row-wise macros. ([#272](https://github.com/JuliaData/DataFramesMeta.jl/pull/272))
* Add row-wise transformation macros `@rtransform`, `@rselect`, `@rsubset`, and `@rorderby`. ([#267](https://github.com/JuliaData/DataFramesMeta.jl/pull/267))
* Add`$` to escape columns rather than `cols`, which is now deprecated. ([#285](https://github.com/JuliaData/DataFramesMeta.jl/pull/285))
* Fix a bug disallowing multiple arguments in function-like syntax for row-wise macros. ([#281](https://github.com/JuliaData/DataFramesMeta.jl/pull/281))
* Documentation improvements ([#277](https://github.com/JuliaData/DataFramesMeta.jl/pull/277), [#279](https://github.com/JuliaData/DataFramesMeta.jl/pull/279), [#284](https://github.com/JuliaData/DataFramesMeta.jl/pull/284), [#286](https://github.com/JuliaData/DataFramesMeta.jl/pull/286))
* Add a new dplyr-inspired tutorial ([#279](https://github.com/JuliaData/DataFramesMeta.jl/pull/279))

# DataFramesMeta v0.6 Release Notes

* The order of rows after a `@transform(gd::GroupedDataFrame, args...)` now matches the 
  order of rows returned after `DataFrames.transform(gd::GroupedDataFrame, args...)`. 
* `@select` now supports `GroupedDataFrame` with the same behavior as 
  `DataFrames.select(df::GroupedDataFrame, args...)` ([#180])
* `@orderby(gd::GroupedDataFrame, args...)` is now reserved and will error.
* Restrictions are imposed on the types of column references allowed when using `cols`. 
  Mixing integer column references with other types now errors. ([#183])
* `@where` with a grouped data frame will now perform operations by group and filter
  rows in the parent `AbstractDataFrame`. The operation no longer filters groups. Returns a 
  fresh `DataFrame`.
 * `@based_on` has been renamed to `@combine`
 * `@byrow` has been renamed to `@eachrow`
