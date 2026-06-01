# DataFramesMeta.jl

[![Coveralls](https://coveralls.io/repos/github/JuliaStats/DataFramesMeta.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaStats/DataFramesMeta.jl?branch=master)
[![CI Testing](https://github.com/JuliaData/DataFramesMeta.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/DataFramesMeta.jl/actions?query=workflow%3ACI+branch%3Amaster)
[![](https://img.shields.io/badge/docs-stable-blue.svg)](https://JuliaData.github.io/DataFramesMeta.jl/stable)
[![](https://img.shields.io/badge/docs-dev-blue.svg)](https://JuliaData.github.io/DataFramesMeta.jl/dev)

A collection of macro functions which simplify the syntax required to operate on DataFrames.jl objects.

# Installation

DataFramesMeta.jl is a registered Julia package. Run either of the following to 
install:

```julia
julia> import Pkg; Pkg.add("DataFramesMeta")
```

or via the `Pkg` REPL mode (enter by typing `]` at the REPL console)

```julia
] add DataFramesMeta
```

# Usage

Instead of using DataFrames.jl [manipulation functions](https://dataframes.juliadata.org/stable/man/basics/#Manipulation-Functions) directly,
which use operation pairs to define data frame manipulations,
DataFramesMeta.jl offers macro alternatives to each manipulation function,
which convert a more readable syntax into the appropriate DataFrames.jl function calls.
This syntax simplification is demonstrated with two simple examples below.

Define a data frame.

```julia
df = DataFrame(a = [1, 2], b = [3, 4]);
```

Add columns `a` and `b` together and store the result in a new column `c`.

```julia
# With DataFrames
transform(df, [:a, :b] => ((x, y) -> x + y) => :c)

# With DataFramesMeta
@transform(df, :c = :a + :b)
```

Show only the data frame rows where column `a` is equal to `2`.
(DataFramesMeta.jl offers rowwise 'r' versions of each manipulation macro as well for convenience.)

```julia
# With DataFrames
subset(df, :a => ByRow(==(2)))

# With DataFramesMeta
@rsubset(df, :a == 2)
```

# Documentation

* [Stable](https://JuliaData.github.io/DataFramesMeta.jl/stable)
* [Development](https://JuliaData.github.io/DataFramesMeta.jl/dev)

# Package Maintenance

Any of the
[JuliaData collaborators](https://github.com/orgs/JuliaData/teams/collaborators)
have write access and can accept pull requests.

Pull requests are welcome. Pull requests should include updated tests. If
functionality is changed, docstrings should be added or updated. Generally,
follow the guidelines in
[DataFrames](https://github.com/JuliaData/DataFrames.jl/blob/master/CONTRIBUTING.md).

# Alternatives

Other high-level data frame manipulation packages are described briefly [here](https://dataframes.juliadata.org/stable/man/querying_frameworks/).
