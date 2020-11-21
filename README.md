# DataFramesMeta.jl

[![Coveralls](https://coveralls.io/repos/github/JuliaStats/DataFramesMeta.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaStats/DataFramesMeta.jl?branch=master)
[![Travis](https://travis-ci.org/JuliaData/DataFramesMeta.jl.svg?branch=master)](https://travis-ci.org/JuliaData/DataFramesMeta.jl)
[![](https://img.shields.io/badge/docs-stable-blue.svg)](https://JuliaData.github.io/DataFramesMeta.jl/stable)
[![](https://img.shields.io/badge/docs-dev-blue.svg)](https://JuliaData.github.io/DataFramesMeta.jl/dev)

Metaprogramming tools for DataFrames.jl objects.

# Macros 

* `@transform`, for adding new columns to a data frame
* `@select`, for selecting columns in a data frame
* `@combine`, for applying operations on each group of a grouped data frame
* `@orderby`, for sorting data frames
* `@where`, for keeping rows of a DataFrame matching a given condition
* `@by`, for grouping and combining a data frame in a single step
* `@with`, for working with the columns of a data frame with high performance and 
  convenient syntax
* `@eachrow`, for looping through rows in data frame, again with high performance and 
  convenient syntax. 
* `@linq`, for piping the above macros together, similar to [magrittr](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html)'s
  `%>%` in R. 

# Installation

DataFramesMeta.jl is a registered Julia package. Run either of the following to 
install:

```
julia> import Pkg; Pkg.add("DataFramesMeta")
```

or via the `Pkg` REPL mode (enter by typing `]` at the REPL console)

```
] add DataFramesMeta
```

# Documentation

* [Stable](https://JuliaData.github.io/DataFramesMeta.jl/stable)
* [Development](https://JuliaData.github.io/DataFramesMeta.jl/dev)

# Package Maintenance

Any of the
[JuliaDatacollaborators](https://github.com/orgs/JuliaData/teams/collaborators)
have write access and can accept pull requests.

Pull requests are welcome. Pull requests should include updated tests. If
functionality is changed, docstrings should be added or updated. Generally,
follow the guidelines in
[DataFrames](https://github.com/JuliaData/DataFrames.jl/blob/master/CONTRIBUTING.md).