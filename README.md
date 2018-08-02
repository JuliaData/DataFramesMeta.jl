# DataFramesMeta.jl

[![DataFramesMeta](http://pkg.julialang.org/badges/DataFramesMeta_0.6.svg)](http://pkg.julialang.org/?pkg=DataFramesMeta?pkg=DataFramesMeta&ver=0.6)
[![Coveralls](https://coveralls.io/repos/github/JuliaStats/DataFramesMeta.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaStats/DataFramesMeta.jl?branch=master)
[![Travis](https://travis-ci.org/JuliaStats/DataFramesMeta.jl.svg?branch=master)](https://travis-ci.org/JuliaStats/DataFramesMeta.jl)
[![AppVeyor](https://ci.appveyor.com/api/projects/status/github/juliastats/dataframesmeta.jl?branch=master&svg=true)](https://ci.appveyor.com/project/tshort/dataframesmeta-jl/branch/master)

Metaprogramming tools for DataFrames.jl and `AbstractDict` objects.
These macros improve performance and provide more convenient syntax.

# Features

## `@with`

`@with` allows DataFrame columns to be referenced as symbols like
`:colX` in expressions. If an expression is wrapped in `^(expr)`,
`expr` gets passed through untouched. If an expression is wrapped in
`cols(expr)`, the column is referenced by the variable `expr` rather than
a symbol. Here are some examples:

```julia
using DataFrames
using DataFramesMeta

df = DataFrame(x = 1:3, y = [2, 1, 2])
x = [2, 1, 0]

@with(df, :y .+ 1)
@with(df, :x + x)  # the two x's are different

x = @with df begin
    res = 0.0
    for i in 1:length(:x)
        res += :x[i] * :y[i]
    end
    res
end

@with(df, df[:x .> 1, ^(:y)]) # The ^ means leave the :y alone

colref = :x
@with(df, :y + cols(colref)) # Equivalent to df[:y] + df[colref]
```

This works for `AbstractDict` types, too:

```julia
y = 3
d = Dict(:s => 3, :y => 44, :d => 5)

@with(d, :s + :y + y)
```

`@with` is the fundamental macro used by the other metaprogramming
utilities.

`@with` creates a function, so scope within `@with` is a local scope.
Variables in the parent can be read. Writing to variables in the parent scope
differs depending on the type of scope of the parent. If the parent scope is a
global scope, then a variable cannot be assigned without using the `global` keyword.
If the parent scope is a local scope (inside a function or let block for example),
the `global` keyword is not needed to assign to that parent scope.

## `@where`

Select row subsets.

```julia
@where(df, :x .> 1)
@where(df, :x .> x)
@where(df, :x .> x, :y .== 3)  # the two expressions are "and-ed"
```

## `@select`

Column selections and transformations. Also works with `AbstractDict` types.

```julia
@select(df, :x, :y, :z)
@select(df, x2 = 2 * :x, :y, :z)
```

## `@transform`

Add additional columns based on keyword arguments.

```julia
@transform(df, newCol = cos.(:x), anotherCol = :x.^2 + 3*:x .+ 4)
```

`@transform` works for `AbstractDict` types, too.

## `@byrow!`

Act on a DataFrame row-by-row. Includes support for control flow and `begin end` blocks. Since the "environment" induced by `@byrow! df` is implicitly a single row of `df`, one uses regular operators and comparisons instead of their elementwise counterparts as in `@with`.

```julia
@byrow! df if :A > :B; :A = :B * :C end
```
```julia
let x = 0.0
    @byrow! df begin
        if :A < :B
            x += :B * :C
        end
    end
    x
end
```

Note that the let block is required here to create a scope to allow assignment
of `x` within `@byrow!`.

`byrow!` also supports special syntax for allocating new columns to make
`byrow!` more useful for data transformations. The syntax `@newcol
x::Array{Int}` allocates a new column `:x` with an `Array` container with eltype
`Int`. Note that the returned `AbstractDataFrame` includes these new columns, but
the original `df` is not affected. Here is an example where two new columns are
added:

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
df2 = @byrow! df begin
    @newcol colX::Array{Float64}
    @newcol colY::Array{Union{Int,Missing}}
    :colX = :B == 2 ? pi * :A : :B
    if :A > 1
        :colY = :A * :B
    else
        :colY = Missing
    end
end
```

## LINQ-Style Queries and Transforms

A number of functions for operations on DataFrames have been defined.
Here is a table of equivalents for Hadley's
[dplyr](https://github.com/hadley/dplyr) and common
[LINQ](http://en.wikipedia.org/wiki/Language_Integrated_Query)
functions.

    Julia             dplyr            LINQ
    ---------------------------------------------
    @where            filter           Where
    @transform        mutate           Select (?)
    @by                                GroupBy
    groupby           group_by
    @based_on         summarise/do
    @orderby          arrange          OrderBy
    @select           select           Select


## LINQ macro

There is also a `@linq` macro that supports chaining and all of the
functionality defined in other macros. Here is an example of `@linq`:

```julia
x_thread = @linq df |>
    transform(y = 10 * :x) |>
    where(:a .> 2) |>
    by(:b, meanX = mean(:x), meanY = mean(:y)) |>
    orderby(:meanX) |>
    select(:meanX, :meanY, var = :b)
```

Relative to the use of individual macros, chaining looks cleaner and
more obvious with less noise from `@` symbols. This approach also
avoids filling up the limited macro name space. The main downside is
that more magic happens under the hood.

This method is extensible. Here is a comparison of the macro and
`@linq` versions of `with`.

```julia
macro with(d, body)
    esc(with_helper(d, body))
end

function linq(::SymbolParameter{:with}, d, body)
    with_helper(d, body)
end
```

The `linq` method above registers the expression-replacement method
defined for all `with()` calls. It should return an expression like a
macro.

Again, this is experimental. Based on feedback, we may decide to only
use `@linq` or only support the set of linq-like macros.

## Operations on GroupedDataFrames

The following operations are now included:

- `where(g, d -> mean(d[:a]) > 0)` and `@where(g, mean(:a) > 0)` --
  Filter groups based on the given criteria. Returns a
  GroupedDataFrame.

- `orderby(g, d -> mean(d[:a]))` and `@orderby(g, mean(:a))` -- Sort
  groups based on the given criteria. Returns a GroupedDataFrame.

- `DataFrame(g)` -- Convert groups back to a DataFrame with the same
  group orderings.

- `@based_on(g, z = mean(:a))` -- Summarize results within groups.
  Returns a DataFrame.

- `transform(g, d -> y = d[:a] - mean(d[:a]))` and
  `@transform(g, y = :a - mean(:a))` -- Transform a DataFrame based
  on operations within a group. Returns a DataFrame.

You can also index on GroupedDataFrames. `g[1]` is the first group,
returned as a SubDataFrame. `g[[1,4,5]]` or
`g[[true, false, true, false, false]]` return subsets of groups as a
GroupedDataFrame. You can also iterate over GroupedDataFrames.

The most general split-apply-combine approach is based on `map`.
`map(fun, g)` returns a GroupApplied object with keys and vals. This
can be used with `combine`.


# Performance

`@with` works by parsing the expression body for all columns indicated
by symbols (e.g. `:colA`). Then, a function is created that wraps the
body and passes the columns as function arguments. This function is
then called. Operations are efficient because:

- A pseudo-anonymous function is defined, so types are stable.
- Columns are passed as references, eliminating DataFrame indexing.

All of the other macros are based on `@with`.


# Package Maintenance

Any of the
[JuliaStats collaborators](https://github.com/orgs/JuliaStats/teams/collaborators)
have write access and can accept pull requests.

Pull requests are welcome. Pull requests should include updated tests. If
functionality is changed, docstrings should be added or updated. Generally,
follow the guidelines in
[DataFrames](https://github.com/JuliaStats/DataFrames.jl/blob/master/CONTRIBUTING.md).
