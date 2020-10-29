# DataFramesMeta.jl

[![DataFramesMeta](http://pkg.julialang.org/badges/DataFramesMeta_0.6.svg)](http://pkg.julialang.org/?pkg=DataFramesMeta?pkg=DataFramesMeta&ver=0.6)
[![Coveralls](https://coveralls.io/repos/github/JuliaStats/DataFramesMeta.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaStats/DataFramesMeta.jl?branch=master)
[![Travis](https://travis-ci.org/JuliaStats/DataFramesMeta.jl.svg?branch=master)](https://travis-ci.org/JuliaStats/DataFramesMeta.jl)
[![AppVeyor](https://ci.appveyor.com/api/projects/status/github/juliastats/dataframesmeta.jl?branch=master&svg=true)](https://ci.appveyor.com/project/tshort/dataframesmeta-jl/branch/master)

Metaprogramming tools for DataFrames.jl objects.
These macros improve performance and provide more convenient syntax.

DataFrames.jl has the functions `select`, `transform`, and `combine` 
for manipulating data frames. DataFramesMeta provides the macros 
`@select`, `@transform`, and `@combine` that provides an easier syntax, 
inspired by [dplyr](https://dplyr.tidyverse.org/) in R for interfacing with 
their corresponding DataFrames functions. 

In addition, DataFramesMeta provides 

* `@orderby`, for sorting data frames
* `@where`, for keeping rows of a DataFrame matching a given condition
* `@by`, for grouping and combining a data frame in a single step
* `@with`, for working with the columns of a data frame with high performance and 
  convenient syntax
* `@eachrow`, for looping through rows in data frame, again with high performance and 
  convenient syntax. 
* `@linq`, for piping the above macros together, similar to [magittr](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html)'s
  `%>%` in R. 

To reference columns inside DataFramesMeta macros, use `Symbol`s. For example, use `:x`
to refer to the column `df.x`. To use a variable `varname` representing a `Symbol` to refer to 
a column, use the syntax `cols(varname)`. Details below: 

# Details

## `@select`

Column selections and transformations. Only newly created columns are kept. 
Operates on both a `DataFrame` and a `GroupedDataFrame`. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@select(df, :x, :y)
@select(df, x2 = 2 * :x, :y)
@select(gd, x2 = 2 .* :y .* first(:y))
```

!!! note 
    
    Versions of DataFrames.jl `0.19` and above support the operators `Between`, `All`,
    and `Not` when selecting and transforming columns. DataFramesMeta.jl does not currently
    support this syntax. 

!!! note
    To refer to `Symbol`s without aliasing the column in a DataFame, use `^`. 

    ```
    df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
    @select(df, :x2 = :x, :x3 = ^(:x))
    ```

    This rule applies to all DataFramesMeta macros.

## `@transform`

Add additional columns based on keyword arguments. Operates on both a 
`DataFrame` and a `GroupedDataFrame`. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@transform(df, :x, :y)
@transform(df, x2 = 2 * :x, :y)
@transform(gd, x2 = 2 .* :y .* first(:y))
```

## `@where`

Select row subsets. Operates on both a `DataFrame` and a `GroupedDataFrame`. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
x = 1;
@where(df, :x .> 1)
@where(df, :x .> x)
@where(df, :x .> x, :y .< 102)  # the two expressions are "and-ed"
@where(gd, :x .> mean(:x))
```

## `@combine`

Summarize or collapse, a grouped data frame by perforing transformations at the group level and 
collecting the result into a single data frame. Also works on a `DataFrame`, which 
acts like a `GroupedDataFrame` with one group. 

Does not allow input in first argument like `combine` from DataFrames.jl. But an argument
returning a `Table`-like object can be input for the first argument. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
using Statistics
@combine(gd, x2 = mean(:y))
@combine(gd, x2 = :y .- mean(:y))
@combine(gd, @combine(gd, (n1 = mean(:y), n2 = first(:y))))
```

## `@orderby`

Sort rows in a `DataFrame` by values in one of several columns or a 
transformation of columns. Only operates on `DataFrame`s and not `GroupedDataFrame`s. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
@orderby(df, -1 .* :x)
@orderby(df, :x, :y .- mean(:y))
```

## `@with`

`@with` creates a scope in which all symbols that appear are aliases for the columns
in a DataFrame. 

```julia
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

```

!!! note 
    `@with` creates a function, so scope within `@with` is a local scope.
    Variables in the parent can be read. Writing to variables in the parent scope
    differs depending on the type of scope of the parent. If the parent scope is a
    global scope, then a variable cannot be assigned without using the `global` keyword.
    If the parent scope is a local scope (inside a function or let block for example),
    the `global` keyword is not needed to assign to that parent scope.

!!! note
    Because `@with` creates a function, be careful with the use of `return`. 

    ```
    function data_transform(df; returnearly = false)
        if returnearly
            @with df begin 
                z = :x + :y
                return z
            end
        else 
            return [1, 2, 3]
        end

        return [4, 5, 6]
    end
    ```

    The above function will return `[4, 5, 6]` because the `return` inside the `@with`
    applies to the anonymous function created by `@with`. 

    Given that `@eachrow` (below) is implemented with `@with`, the same caveat applies to 
    `@eachrow` blocks. 


## `@eachrow`

Act each row of a data frame. Includes support for control flow and `begin end` blocks. Since the "environment" induced by `@eachrow df` is implicitly a single row of `df`, one uses regular operators and comparisons instead of their elementwise counterparts as in `@with`. Does not change the input data frame argument.

```julia
df2 = @eachrow df if :A > :B; :A = :B * :C end
```
```julia
let x = 0.0
    @eachrow df begin
        if :A < :B
            x += :B * :C
        end
    end
    x
end
```

Note that the let block is required here to create a scope to allow assignment
of `x` within `@eachrow`.

`eachrow` also supports special syntax for allocating new columns to make
`eachrow` more useful for data transformations. The syntax `@newcol
x::Array{Int}` allocates a new column `:x` with an `Array` container with eltype
`Int`. Here is an example where two new columns are added:

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
df2 = @eachrow df begin
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

## Working with column names programmatically with `cols`

DataFramesMeta.jl provides the special syntax `cols` for referring to 
columns in a data frame via a `Symbol`, string, or column position as either
a literal or a variable. 

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])

nameA = :A
df2 = @transform(df, C = :B - cols(nameA))

nameA_string = "A"
df3 = @transform(df, C = :B - cols(nameA_string))

nameB = "B"
df4 = @eachrow df begin 
    :A = cols(nameB)
end
```

`cols` can also be used to create new columns in a data frame. 

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])

newcol = "C"
@select(df, cols(newcol) = :A + :B)

@by(df, :B, cols("A complicated" * " new name") = first(:A))

nameC = "C"
df3 = @eachrow df begin 
    @newcol cols(nameC)::Vector{Int}
    cols(nameC) = :A
end
```

DataFramesMeta macros do not allow mixing of integer column references with references 
of other types. This means `@transform(df, y = :A + cols(2))`, attempting to add the columns 
`df[!, :A]` and `df[!, 2]`, will fail. This is because in DataFrames, the command 

```julia
transform(df, [:A, 2] => (+) => :y)
``` 

will fail, as DataFrames requires the "source" column identifiers in a 
`source => fun => dest` pair to all have the same type. DataFramesMeta adds one exception
to this rule. `Symbol`s and strings are allowed to be mixed inside DataFramesMeta macros. 
Consequently, 

```
@transform(df, y = :A + cols("B"))
```

will not error even though 

```
transform(df, [:A, "B"] => (+) => :y)
```

will error in DataFrames. 

For consistency, this restriction in the input column types also applies to `@with`
and `@eachrow`. You cannot mix integer column references with `Symbol` or string column 
references in `@with` and `@eachrow` in any part of the expression, but you can mix 
`Symbol`s and strings. The following will fail:

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
@eachrow df begin 
    :A = cols(2)
end

@with df begin 
    cols(1) + cols("A")
end
```

while the following will work without error

```julia
@eachrow df begin 
    cols(1) = cols(2)
end

@with df begin 
    cols(1) + cols(2)
end
```

Note that `cols` is *not* a standard Julia function. It is only used to modify the 
way that macros in DataFramesMeta.jl escape arguments and has no behavior of its own 
outside of DataFramesMeta macros.

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
    @combine         summarise/do
    @orderby          arrange          OrderBy
    @select           select           Select


## `@linq` and other chaining macros

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

Alternatively you can use Lazy.jl `@>` macro like this:

```julia
using Lazy: @>
x_thread = @> begin
    df
    @transform(y = 10 * :x)
    @where(:a .> 2)
    @by(:b, meanX = mean(:x), meanY = mean(:y))
    @orderby(:meanX)
    @select(:meanX, :meanY, var = :b)
end
```

Please note that Lazy.jl exports the function `groupby` which would clash
with `DataFrames.groupby`. Hence, it is recommended that you only import a
select number of functions into the namespace by only importing `@>` e.g. 
`using Lazy: @>` instead of `using Lazy`.

Another alternative is Pipe.jl which exports the `@pipe` macro for piping. 
The piping mechanism in Pipe.jl requires explicit specification of the piped
object via `_` instead of assuming it is the first argument to the next function.
The Pipe.jl equivalent of the above is:

```julia
using Pipe
x_thread = @pipe df |>
    @transform(_, y = 10 * :x) |>
    @where(_, :a .> 2) |>
    @by(_, :b, meanX = mean(:x), meanY = mean(:y)) |>
    @orderby(_, :meanX) |>
    @select(_, :meanX, :meanY, var = :b)
```

# Package Maintenance

Any of the
[JuliaStats collaborators](https://github.com/orgs/JuliaStats/teams/collaborators)
have write access and can accept pull requests.

Pull requests are welcome. Pull requests should include updated tests. If
functionality is changed, docstrings should be added or updated. Generally,
follow the guidelines in
[DataFrames](https://github.com/JuliaStats/DataFrames.jl/blob/master/CONTRIBUTING.md).
