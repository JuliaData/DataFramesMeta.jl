# DataFramesMeta v0.15.0 Release notes

* Added `@note!` and `@label!`, along with `printlabels` and `printnotes` to make working with metadata easier. ([#377](https://github.com/JuliaData/DataFramesMeta.jl/pull/377))
* Added support for `Not`, `All`, and `Cols` in `@select`, making it easier to keep or drop many columns at once. ([#372](https://github.com/JuliaData/DataFramesMeta.jl/pull/372))
* Added the `@groupby` macro, which provides an easier syntax for grouping data frames ([#373](https://github.com/JuliaData/DataFramesMeta.jl/pull/373))

# DataFramesMeta v0.14.1 Release notes
* Fixed a bug where `ByRow` was not properly identified if DataFramesMeta.jl was `import`ed ([#366](https://github.com/JuliaData/DataFramesMeta.jl/pull/366))

# DataFramesMeta v0.14.0 Release notes

* Add the `@rename` macro for renaming columns in keyword argument-like syntax. ([#343](https://github.com/JuliaData/DataFramesMeta.jl/pull/343))
* Fix a bug relating to `^` for escaping symbols ([#347](https://github.com/JuliaData/DataFramesMeta.jl/pull/347))
* Fix typos in documentation ([#355](https://github.com/JuliaData/DataFramesMeta.jl/pull/355))

# DataFramesMeta v0.13.0 Release notes

* Add the `@distinct` and `@rdistinct` macros, for getting unique observations of a data frame. ([#340](https://github.com/JuliaData/DataFramesMeta.jl/pull/340))
* Fix a bug which created `UndefVarErrors` with broadcasted functions. ([#346](https://github.com/JuliaData/DataFramesMeta.jl/pull/346))
* Fixed bad use of `sortperm` with correct use of `ordinalrank` from StatsBase.jl in tutorial ([#338](https://github.com/JuliaData/DataFramesMeta.jl/pull/338))
* Minor documentation fixes ([#345](https://github.com/JuliaData/DataFramesMeta.jl/pull/345))

# DataFramesMeta v0.12.0 Release notes
* Add support for Chain.jl version 0.5, and remove support for Chain.jl 0.4. In Chain.jl 0.4, the command

  ```
  chain df begin 
      f(df)
      @aside x = 1
  end
  ```
  
  creates a `let` scope and thus `x` is not visible outside the `@chain` block. In version 0.5, the above macro does *not* create a `let` scope, making `x` accessible outside the block. To restore 0.4 behavior, write `@chain let ...`. Because this a breaking change of a dependency, we also release a version bump of DataFramesMeta.jl. ([#332](https://github.com/JuliaData/DataFramesMeta.jl/pull/332))

# DataFramesMeta v0.11.0 Release notes

* Allow `AsTable` on the RHS of transformations. This allows one to work with collections of columns programtically, such as taking the row-wise `mean` of many columns. ([#307](https://github.com/JuliaData/DataFramesMeta.jl/pull/307))
* Expressions on the RHS of the form `f ∘ g` will now be passed directly to the underlying `transform` call without modification, reducing compilation latency. ([#317](https://github.com/JuliaData/DataFramesMeta.jl/pull/317))
* Nested functions, of the form `f(g(:x))` will be transformed to `(f ∘ g)(:x)`, further reducing latency. ([#319](https://github.com/JuliaData/DataFramesMeta.jl/pull/319))
* Improvements to documentation ([#305](https://github.com/JuliaData/DataFramesMeta.jl/pull/305), [#314](https://github.com/JuliaData/DataFramesMeta.jl/pull/314), [#315](https://github.com/JuliaData/DataFramesMeta.jl/pull/315), [#318](https://github.com/JuliaData/DataFramesMeta.jl/pull/318), [#322](https://github.com/JuliaData/DataFramesMeta.jl/pull/322), [#326](https://github.com/JuliaData/DataFramesMeta.jl/pull/326))

# DataFramesMeta v0.10.0 Release notes

* Add the `@astable` macro-flag to construct multiple inter-dependent columns at once. ([#298](https://github.com/JuliaData/DataFramesMeta.jl/pull/298)). 
* As a result of #298, automatic `AsTable` expansion in non-keyword transformations in the first (and only) transformations in `@by` and `@combine` are no longer supported. Previously, such operations were allowed, but with a visible deprecation warning.

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
