# DataFramesMeta.jl

[![DataFramesMeta](http://pkg.julialang.org/badges/DataFramesMeta_0.4.svg)](http://pkg.julialang.org/?pkg=DataFramesMeta&ver=0.4)
[![DataFramesMeta](http://pkg.julialang.org/badges/DataFramesMeta_0.5.svg)](http://pkg.julialang.org/?pkg=DataFramesMeta)
[![Coverage Status](http://img.shields.io/coveralls/JuliaStats/DataFramesMeta.jl.svg)](https://coveralls.io/r/JuliaStats/DataFramesMeta.jl)
[![Build Status](https://travis-ci.org/JuliaStats/DataFramesMeta.jl.svg?branch=master)](https://travis-ci.org/JuliaStats/DataFramesMeta.jl)

Metaprogramming tools for DataFrames and Associative objects.
These macros improve performance and provide more convenient syntax.

# Features

## `@with`

`@with` allows DataFrame columns to be referenced as symbols like
`:colX` in expressions. If an expression is wrapped in `^(expr)`,
`expr` gets passed through untouched. If an expression is wrapped in 
`_I_(expr)`, the column is referenced by the variable `expr` rather than
a symbol. Here are some examples:

```julia
using DataArrays, DataFrames
using DataFramesMeta

df = DataFrame(x = 1:3, y = [2, 1, 2])
x = [2, 1, 0]

@with(df, :y + 1)
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
@with(df, :y + _I_(colref)) # Equivalent to df[:y] + df[colref]

```

This works for Associative types, too:

```julia
y = 3
d = Dict(:s => 3, :y => 44, :d => 5)

@with(d, :s + :y + y)
```

`@with` is the fundamental macro used by the other metaprogramming
utilities.

`@with` creates a function, so scope within `@with` is a [hard
scope](http://docs.julialang.org/en/release-0.4/manual/variables-and-scoping/#hard-local-scope),
as with `do`-blocks or other function definitions. Variables in the parent
can be read. Writing to variables in the parent scope differs depending on
the type of scope of the parent. If the parent scope is a global scope, then
a variable cannot be assigned without using the `global` keyword. If the parent
scope is a local scope (inside a function or let block for example), the `global`
keyword is not needed to assign to that parent scope.

## `@where`

Select row subsets.

```julia
@where(df, :x .> 1)
@where(df, :x .> x)
@where(df, :x .> x, :y .== 3)  # the two expressions are "and-ed"
```

## `@select`

Column selections and transformations. Also works with Associative types.

```julia
@select(df, :x, :y, :z)
@select(df, x2 = 2 * :x, :y, :z)
```

## `@transform`

Add additional columns based on keyword arguments. This is available
in both function and macro versions with the macro version allowing
direct reference to columns using the colon syntax:

```julia
transform(df, newCol = cos(df[:x]), anotherCol = df[:x]^2 + 3*df[:x] + 4)
@transform(df, newCol = cos(:x), anotherCol = :x^2 + 3*:x + 4)
```

`@transform` works for Associative types, too.

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
`Int`. Note that the returned AbstractDataFrame includes these new columns, but
the original `df` is not affected. Here is an example where two new columns are
added:

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
df2 = @byrow! df begin
    @newcol colX::Array{Float64}
    @newcol colY::DataArray{Int}
    :colX = :B == 2 ? pi * :A : :B
    if :A > 1 
        :colY = :A * :B
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
    @groupby          group_by
    @based_on         summarise/do
    @orderby          arrange          OrderBy
    @select           select           Select


Chaining operations is a useful way to manipulate data. There are
several ways to do this. This is still in flux in base Julia
(https://github.com/JuliaLang/julia/issues/5571). Here is one option
from [Lazy.jl](https://github.com/one-more-minute/Lazy.jl) by Mike
Innes:

```julia
x_thread = @> begin
    df
    @transform(y = 10 * :x)
    @where(:a .> 2)
    @by(:b, meanX = mean(:x), meanY = mean(:y))
    @orderby(:meanX)
    @select(:meanX, :meanY, var = :b)
end
```

## Alternative LINQ macro

As another experiment, there is also a `@linq` macro that supports
chaining and all of the functionality defined in other macros. Here is
an example of `@linq`:

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

# CompositeDataFrame

A `CompositeDataFrame` is a type-stable `AbstractDataFrame` built using composite
types. Each column is a field in a composite type. `CompositeDataFrame` is an
abstract type; each concrete composite type inherits from this. The advantages
of this approach are:

* You can access single columns directly using `df.colA`. This is type stable,
  so code should be faster. (There is still the function boundary to worry 
  about.)

* All indexing operations can be done currently.

Some downsides include:

* As an abuse of the type system, creating a new type for each change to
  a `CompositeDataFrame` may waste memory.

* You cannot change the structure of a `CompositeDataFrame` once created.
  It is nearly like an immutable object. For example to add a column, you need 
  to do something like:

```julia
    transform(df, newcol = df.colA + 5)
```
    
An advantage of this is that the API becomes more functional. All
manipulations of the `CompositeDataFrame` return a new object.
Normally, this doesn't create much more memory.

To create a CompositeDataFrame, use `CompositeDataFrame`:
  
```julia
n = 10
d = CompositeDataFrame(a = 1:n, b = rand(10), c = DataArray(rand(1:3, n)))
```

Note that `CompositeDataFrame()` does not coerce to `DataArrays`. Ranges and other
`AbstractVectors` are left as is, so convert to `DataArray` or `NullableArray` as
appropriate.

You can also name the type of the `CompositeDataFrame` by including that as the
first symbol:
  
```julia
n = 10
d = CompositeDataFrame(:MyDF, a = 1:n, b = rand(n), c = DataArray(rand(1:3, n)))
```

You can also define a `CompositeDataFrame` manually as follows. If you do this,
you are responsible for keeping each column the same length.

```julia
immutable MyDF <: AbstractCompositeDataFrame
    a::Vector{Int}
    b::Vector{Float64}
    c::DataVector{Float64}
end

MyDF(n::Integer) = MyDF(zeros(Int, n), zeros(n), DataArray(zeros(n)))
d = MyDF(10)
```

Note that a `CompositeDataFrame` is type stable with field access like `df.colA` 
but not with `getindex` indexing like `df[:colA]`. `df[:colA]` works, but it is
not type stable.

Type-stable access to rows is also provided using `row(d, i)` or the iterator
`eachrow(d)`. Here is an example:

```julia
n = 10
d = CompositeDataFrame(:MyDF, a = 1:n, b = rand(10), c = DataArray(rand(1:3, n)))
x = row(d, 5)
x.a    # 5
y = [x.a * x.b for x in eachrow(d)]
```

In the example above, the call to `CompositeDataFrame` creates the type `MyDF`
that holds the composite data frame and another type `MyDFRow` that is used by
`row` and `eachrow`. 

# Package Maintenance

[Tom Short](https://github.com/tshort) is the lead maintainer. Any of the 
[JuliaStats collaborators](https://github.com/orgs/JuliaStats/teams/collaborators) 
also have write access and can accept pull requests. 

Pull requests are welcome. Pull requests should include updated tests. If
functionality is changed, docstrings should be added or updated. Generally,
follow the guidelines in
[DataFrames](https://github.com/JuliaStats/DataFrames.jl/blob/master/CONTRIBUTING.md).
