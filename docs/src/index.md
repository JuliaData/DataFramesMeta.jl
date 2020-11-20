# Introduction

Metaprogramming tools for DataFrames.jl objects to provide more convenient syntax.

DataFrames.jl has the functions `select`, `transform`, and `combine` 
for manipulating data frames. DataFramesMeta provides the macros 
`@select`, `@transform`, and `@combine` to mirror these functions with 
more convenient syntax. Inspired by [dplyr](https://dplyr.tidyverse.org/) in R 
and [LINQ](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/)
in C#. 

In addition, DataFramesMeta provides 

* `@orderby`, for sorting data frames
* `@where`, for keeping rows of a DataFrame matching a given condition
* `@by`, for grouping and combining a data frame in a single step
* `@with`, for working with the columns of a data frame with high performance and 
  convenient syntax
* `@eachrow`, for looping through rows in data frame, again with high performance and 
  convenient syntax. 
* `@linq`, for piping the above macros together, similar to [magrittr](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html)'s
  `%>%` in R. 

See below the convenience of DataFramesMeta compared to DataFrames.

```julia
df = DataFrame(a = [1, 2], b = [3, 4]);

# With DataFrames
transform(df, [:a, :b] => ((a, b) -> a .* b .+ first(a) .- sum(b)) => :c);

# With DataFramesMeta
@transform(df, c = :a .* :b .+ first(:a) .- sum(:b))
```

To reference columns inside DataFramesMeta macros, use `Symbol`s. For example, use `:x`
to refer to the column `df.x`. To use a variable `varname` representing a `Symbol` to refer to 
a column, use the syntax `cols(varname)`. 

Use `passmissing`  to propagate `missing` values more easily. See `?passmissing` for 
details. `passmissing` is defined in [Missings.jl](https://github.com/JuliaData/Missings.jl)
but exported by DataFramesMeta for convenience. 

# Provided macros

!!! note 
    
    Newer versions of DataFrames.jl support the operators `Between`, `All`, `Cols`,
    and `Not` when selecting and transforming columns. DataFramesMeta does not currently
    support this syntax. 

## `@select`

Column selections and transformations. Only newly created columns are kept. 
Operates on both a `DataFrame` and a `GroupedDataFrame`. 

When given a `GroupedDataFrame`, performs a transformation by group and then 
if necessary repeats the result to have as many rows as the input 
data frame. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@select(df, :x, :y)
@select(df, x2 = 2 * :x, :y)
@select(gd, x2 = 2 .* :y .* first(:y))
```
## `@transform`

Add additional columns based on keyword arguments. Operates on both a 
`DataFrame` and a `GroupedDataFrame`. 

When given a `GroupedDataFrame`, performs a transformation by group and then 
if necessary repeats the result to have as many rows as the input 
data frame. 

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
outside_var = 1;
@where(df, :x .> 1)
@where(df, :x .> outside_var)
@where(df, :x .> outside_var, :y .< 102)  # the two expressions are "and-ed"
@where(gd, :x .> mean(:x))
```

## `@combine`

Summarize, or collapse, a grouped data frame by performing transformations at the group level and 
collecting the result into a single data frame. Also works on a `DataFrame`, which 
acts like a `GroupedDataFrame` with one group. 

Examples:

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@combine(gd, x2 = sum(:y))
@combine(gd, x2 = :y .- sum(:y))
@combine(gd, (n1 = sum(:y), n2 = first(:y)))
```

Requires a `DataFrame` or `GroupedDataFrame` as the first argument, unlike 
`combine` from DataFrames.jl. For instance, `@combine((a = sum(:x), b = sum(:y)), gd)` 
will fail because `@combine` requires a `GroupedDataFrame` or a `DataFrame` 
as the first argument. The following, however, will work.

```
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@combine(gd, (a = sum(:x), b = sum(:y)))
```

For arguments which return a table-like object, such as `(a = sum(:x), b = sum(:y))`, above,
`@combine` only allows *one* argument and it must be the *second* positional argument. 
Consider the call 

```
@combine(gd, (a = sum(:x), b = sum(:y)), c = first(:x))
```

the above will fail because `@combine` does not accept a "keyword argument"-style column 
creation after a "return a table"-style column creation call. 


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

Act on each row of a data frame. Includes support for control flow and `begin end` 
blocks. Since the "environment" induced by `@eachrow df` is implicitly a 
single row of `df`, one uses regular operators and comparisons instead of 
their elementwise counterparts as in `@with`. Does not change the input data 
frame argument.

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
df2 = @eachrow df begin 
    :A = :B + 1
end
```

`@eachrow` introduces a function scope, so a `let` block is required here to create 
a scope to allow assignment of variables within `@eachrow`. 

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
let x = 0.0
    @eachrow df begin
        if :A < :B
            x += :B * :C
        end
    end
    x
end
```

`@eachrow` also supports special syntax for allocating new columns to make
`@eachrow` more useful for data transformations. The syntax `@newcol
x::Vector{Int}` allocates a new column `:x` with an `Vector` container with eltype
`Int`. Here is an example where two new columns are added:

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
df2 = @eachrow df begin
    @newcol colX::Vector{Float64}
    @newcol colY::Vector{Union{Int,Missing}}
    :colX = :B == 2 ? pi * :A : :B
    if :A > 1
        :colY = :A * :B
    else
        :colY = Missing
    end
end
```

## Working with column names programmatically with `cols`

DataFramesMeta provides the special syntax `cols` for referring to 
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
way that macros in DataFramesMeta escape arguments and has no behavior of its own 
outside of DataFramesMeta macros.


# Working with `Symbol`s without referring to columns

To refer to `Symbol`s without aliasing the column in a data frame, use `^`. 

```
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
@select(df, :x2 = :x, :x3 = ^(:x))
```

This rule applies to all DataFramesMeta macros.

## Comparison with `dplyr` and LINQ

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
    groupby           group_by         GroupBy
    @combine          summarise/do
    @orderby          arrange          OrderBy
    @select           select           Select


## `@linq` and other chaining macros

There is also a `@linq` macro that supports chaining and all of the
functionality defined in other macros. Here is an example of `@linq`:

```julia
df = DataFrame(a = repeat(1:5, outer = 20),
               b = repeat(["a", "b", "c", "d"], inner = 25),
               x = repeat(1:20, inner = 5))

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

Alternatively you can use Lazy.jl `@>` macro like this:

```julia
using Lazy: @>

df = DataFrame(a = repeat(1:5, outer = 20),
               b = repeat(["a", "b", "c", "d"], inner = 25),
               x = repeat(1:20, inner = 5))

x_thread = @> begin
    df
    @transform(y = 10 * :x)
    @where(:a .> 2)
    @by(:b, meanX = mean(:x), meanY = mean(:y))
    @orderby(:meanX)
    @select(:meanX, :meanY, var = :b)
end
```

!!! note 
    Please note that Lazy exports the function `groupby` which would clash
    with `DataFrames.groupby`. Hence, it is recommended that you only import a
    select number of functions into the namespace by only importing `@>` e.g. 
    `using Lazy: @>` instead of `using Lazy`.

Another alternative is Pipe.jl which exports the `@pipe` macro for piping. 
The piping mechanism in Pipe requires explicit specification of the piped
object via `_` instead of assuming it is the first argument to the next function.
The Pipe.jl equivalent of the above is:

```julia
using Pipe

df = DataFrame(a = repeat(1:5, outer = 20),
               b = repeat(["a", "b", "c", "d"], inner = 25),
               x = repeat(1:20, inner = 5))

x_thread = @pipe df |>
    @transform(_, y = 10 * :x) |>
    @where(_, :a .> 2) |>
    @by(_, :b, meanX = mean(:x), meanY = mean(:y)) |>
    @orderby(_, :meanX) |>
    @select(_, :meanX, :meanY, var = :b)
```

```@contents
Pages = ["api/api.md"]
Depth = 3
```