# Introduction

Metaprogramming tools for DataFrames.jl objects to provide more convenient syntax.

DataFrames.jl has the functions `select`, `transform`, and `combine`, as well as the in-place `select!` and `transform!`
for manipulating data frames. DataFramesMeta.jl provides the macros 
`@select`, `@transform`, `@combine`, `@select!`, and `@transform!` to mirror these functions with 
more convenient syntax. Inspired by [dplyr](https://dplyr.tidyverse.org/) in R 
and [LINQ](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/)
in C#. 

In addition, DataFramesMeta provides 

* `@orderby`, for sorting data frames
* `@subset` and `@subset!`, for keeping rows of a data frame matching a given condition
* Row-wise versions of the above macros in the form of `@rtransform`, `@rtransform!`,
  `@rselect`, `@rselect!`, `@rorderby`, `@rsubset`, and `@rsubset!`.
* `@by`, for grouping and combining a data frame in a single step
* `@with`, for working with the columns of a data frame with high performance and 
  convenient syntax
* `@eachrow` and `@eachrow!` for looping through rows in data frame, again with high performance and 
  convenient syntax. 
* `@byrow` for applying functions to each row of a data frame (only supported inside other macros).
* `@passmissing` for propagating missing values inside row-wise DataFramesMeta.jl transformations.
* `@chain`, from [Chain.jl](https://github.com/jkrumbiegel/Chain.jl) for piping the above macros together, similar to [magrittr](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html)'s
  `%>%` in R. 

See below the convenience of DataFramesMeta compared to DataFrames.

```julia
df = DataFrame(a = [1, 2], b = [3, 4]);

# With DataFrames
transform(df, [:a, :b] => ((a, b) -> a .* b .+ first(a) .- sum(b)) => :c);

# With DataFramesMeta
@transform(df, :c = :a .* :b .+ first(:a) .- sum(:b))
```

To reference columns inside DataFramesMeta macros, use `Symbol`s. For example, use `:x`
to refer to the column `df.x`. To use a variable `varname` representing a `Symbol` to refer to 
a column, use the syntax `$varname`. 

Use `passmissing`  to propagate `missing` values more easily. See `?passmissing` for 
details. `passmissing` is defined in [Missings.jl](https://github.com/JuliaData/Missings.jl)
but exported by DataFramesMeta for convenience. 

# Provided macros

!!! note 
    
    Newer versions of DataFrames.jl support the operators `Between`, `All`, `Cols`,
    and `Not` when selecting and transforming columns. DataFramesMeta does not currently
    support this syntax. 

## `@select` and `@select!`

Column selections and transformations. Only newly created columns are kept. 
Operates on both a `DataFrame` and a `GroupedDataFrame`. 

`@select` returns a new data frame with newly allocated columns, while `@select!`
mutates the original data frame and returns it.

When given a `GroupedDataFrame`, performs a transformation by group and then 
if necessary repeats the result to have as many rows as the input 
data frame. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@select(df, :x, :y)
@select(df, :x2 = 2 * :x, :y)
@select(gd, :x2 = 2 .* :y .* first(:y))
@select!(df, :x, :y)
@select!(df, :x = 2 * :x, :y)
@select!(gd, :y = 2 .* :y .* first(:y))
```

## `@transform` and `@transform!`

Add additional columns based on keyword arguments. Operates on both a 
`DataFrame` and a `GroupedDataFrame`. 

`@transform` returns a new data frame with newly allocated columns, while `@transform!`
mutates the original data frame and returns it.

When given a `GroupedDataFrame`, performs a transformation by group and then 
if necessary repeats the result to have as many rows as the input 
data frame. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@transform(df, :x, :y)
@transform(df, :x2 = 2 * :x, :y)
@transform(gd, :x2 = 2 .* :y .* first(:y))
@transform!(df, :x, :y)
@transform!(df, :x = 2 * :x, :y)
@transform!(gd, :y = 2 .* :y .* first(:y))
```

## `@subset` and `@subset!`

Select row subsets. Operates on both a `DataFrame` and a `GroupedDataFrame`. 
`@subset` always returns a freshly-allocated data frame whereas 
`@subset!` modifies the data frame in-place.

```julia
using Statistics
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
outside_var = 1;
@subset(df, :x .> 1)
@subset(df, :x .> outside_var)
@subset(df, :x .> outside_var, :y .< 102)  # the two expressions are "and-ed"
@subset(gd, :x .> mean(:x))
```

## `@combine`

Summarize, or collapse, a grouped data frame by performing transformations at the group level and 
collecting the result into a single data frame. Also works on a `DataFrame`, which 
acts like a `GroupedDataFrame` with one group. 

Examples:

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@combine(gd, :x2 = sum(:y))
@combine(gd, :x2 = :y .- sum(:y))
@combine(gd, $AsTable = (n1 = sum(:y), n2 = first(:y)))
```

Requires a `DataFrame` or `GroupedDataFrame` as the first argument, unlike 
`combine` from DataFrames.jl. For instance, `@combine((a = sum(:x), b = sum(:y)), gd)` 
will fail because `@combine` requires a `GroupedDataFrame` or a `DataFrame` 
as the first argument. The following, however, will work.

```
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@combine(gd, $AsTable = (a = sum(:x), b = sum(:y)))
```

For arguments which return a table-like object, such as `(a = sum(:x), b = sum(:y))`, above,
`@combine` only allows *one* argument and it must be the *second* positional argument. 
Consider the call 

```
@combine(gd, $AsTable = (a = sum(:x), b = sum(:y)), c = first(:x))
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


## `@eachrow` and `@eachrow!`

Act on each row of a data frame. Includes support for control flow and `begin end` 
blocks. Since the "environment" induced by `@eachrow df` is implicitly a 
single row of `df`, one uses regular operators and comparisons instead of 
their elementwise counterparts as in `@with`. Does not change the input data 
frame argument.

`@eachrow!` is identical to `@eachrow` but acts on a data frame in-place, modifying
the input.

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
:x::Vector{Int}` allocates a new column `:x` with an `Vector` container with eltype
`Int`. Here is an example where two new columns are added:

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
df2 = @eachrow df begin
    @newcol :colX::Vector{Float64}
    @newcol :colY::Vector{Union{Int,Missing}}
    :colX = :B == 2 ? pi * :A : :B
    if :A > 1
        :colY = :A * :B
    else
        :colY = missing
    end
end
```

## Row-wise transformations with `@byrow` and `@rtransform`/`@rselect`/etc.

`@byrow` provides a convenient syntax to apply operations by-row,
without having to vectorize manually. Additionally, the macros
`@rtransform`, `@rtransform!`, `@rselect`, `@rselect!`, 
`@rorderby`, `@rsubset`, and `@rsubset!` use `@byrow` by default.

DataFrames.jl provides the function wrapper `ByRow`. `ByRow(f)(x, y)`
is roughly equivalent to `f.(x, y)`. DataFramesMeta.jl allows users 
to construct expressions using `ByRow` function wrapper with the 
syntax `@byrow` or the row-wise macros `@rtransform`, etc.

`@byrow` is not a "real" macro and cannot be used outside of 
DataFramesMeta.jl macros. However its behavior within DataFramesMeta.jl
macros should be indistinguishable from externally defined macros. 
Thought of as a macro `@byrow` accepts a single argument and 
creates an anonymous function wrapped in `ByRow`.  For example,

```julia
@transform(df, @byrow :y = :x == 1 ? true : false)
```

is equivalent to

```julia
transform(df, :x => ByRow(x -> x == 1 ? true, false) => :y)
```

The following macros accept `@byrow`:

* `@transform` and `@transform!`, `@select`, `@select!`, and `@combine`. 
  `@byrow` can be used in the left hand side of expressions, e.g.
  `@select(df, @byrow z = :x * :y)`. 
* `@subset`, `@subset!` and `@orderby`, with syntax of the form `@subset(df, @byrow :x > :y)`
* `@with`, where the anonymous function created by `@with` is wrapped in
  `ByRow`, as in `@with(df, @byrow :x * :y)`.

To avoid writing `@byrow` multiple times when performing multiple
operations, it is allowed to use`@byrow` at the beginning of a block of 
operations. All transformations in the block will operate by row.

```julia
julia> @subset df @byrow begin
           :a > 1
           :b < 5
       end
1×2 DataFrame
 Row │ a      b     
     │ Int64  Int64 
─────┼──────────────
   1 │     2      4
```

`@byrow` can be used inside macros which accept `GroupedDataFrame`s,
however, like with `ByRow` in DataFrames.jl, when `@byrow` is
used, functions do not take into account the grouping, so for
example the result of `@transform(df, @byrow y = f(:x))` and 
`@transform(groupby(df, :g), @byrow :y = f(:x))` is the same.

## Propagating missing values with `@passmissing`

Many Julia functions to not automatically propagate missing values. For instance, 
`parse(Int, missing)` will error. 

Missings.jl provides the `passmissing` function-wrapper to help get around these
roadblocks: `passmissing(f)(args...)` will return `missing` if any of `args` is
missing. Similarly, DataFramesMeta.jl provides the `@passmissing` function to wrap
the anonymous functions created by row-wise transformations in DataFramesMeta.jl 
in `Missings.passmissing`.

The expression 

```julia
@transform df @byrow @passmissing :c = f(:a, :b)
```

is translated to 

```
transform(df, [:a, :b] => ByRow(passmissing(f)) => :c)
```

See more examples below.

```julia
julia> no_missing(x::Int, y::Int) = x + y;

julia> df = DataFrame(a = [1, 2, missing], b = [4, 5, 6])
3×2 DataFrame
 Row │ a        b
     │ Int64?   Int64
─────┼────────────────
   1 │       1      4
   2 │       2      5
   3 │ missing      6

julia> @transform df @passmissing @byrow c = no_missing(:a, :b)
3×3 DataFrame
 Row │ a        b      c
     │ Int64?   Int64  Int64?
─────┼─────────────────────────
   1 │       1      4        5
   2 │       2      5        7
   3 │ missing      6  missing

julia> df = DataFrame(x_str = ["1", "2", missing])
3×1 DataFrame
 Row │ x_str
     │ String?
─────┼─────────
   1 │ 1
   2 │ 2
   3 │ missing

julia> @rtransform df @passmissing x = parse(Int, :x_str)
3×2 DataFrame
 Row │ x_str    x
     │ String?  Int64?
─────┼──────────────────
   1 │ 1              1
   2 │ 2              2
   3 │ missing  missing
```



## Working with column names programmatically with `\$`

DataFramesMeta provides the special syntax `\$` for referring to 
columns in a data frame via a `Symbol`, string, or column position as either
a literal or a variable. 

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])

nameA = :A
df2 = @transform(df, C = :B - $nameA)

nameA_string = "A"
df3 = @transform(df, C = :B - $nameA_string)

nameB = "B"
df4 = @eachrow df begin 
    :A = $nameB
end
```

`\$` can also be used to create new columns in a data frame. 

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])

newcol = "C"
@select(df, $newcol = :A + :B)

@by(df, :B, $("A complicated" * " new name") = first(:A))

nameC = "C"
df3 = @eachrow df begin 
    @newcol $nameC::Vector{Int}
    $nameC = :A
end
```

DataFramesMeta macros do not allow mixing of integer column references with references 
of other types. This means `@transform(df, y = :A + $2)`, attempting to add the columns 
`df[!, :A]` and `df[!, 2]`, will fail. This is because in DataFrames, the command 

```julia
transform(df, [:A, 2] => (+) => :y)
``` 

will fail, as DataFrames requires the "source" column identifiers in a 
`source => fun => dest` pair to all have the same type. DataFramesMeta adds one exception
to this rule. `Symbol`s and strings are allowed to be mixed inside DataFramesMeta macros. 
Consequently, 

```
@transform(df, y = :A + $"B")
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
    :A = $2
end

@with df begin 
    $1 + $"A"
end
```

while the following will work without error

```julia
@eachrow df begin 
    $1 + $2
end

@with df begin 
    $1 + $2
end
```

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
    @subset           filter           Where
    @transform        mutate           Select (?)
    @by                                GroupBy
    groupby           group_by         GroupBy
    @combine          summarise/do
    @orderby          arrange          OrderBy
    @select           select           Select


## Chaining operations together with `@chain`

To enable connecting multiple commands together in 
a pipe, DataFramesMeta.jl re-exports the `@chain` macro from 
[Chain.jl](https://github.com/jkrumbiegel/Chain.jl). 

```julia
using Statistics 

df = DataFrame(a = repeat(1:5, outer = 20),
               b = repeat(["a", "b", "c", "d"], inner = 25),
               x = repeat(1:20, inner = 5))

x_thread = @chain df begin
    @transform(:y = 10 * :x)
    @subset(:a .> 2)
    @by(:b, :meanX = mean(:x), :meanY = mean(:y))
    @orderby(:meanX)
    @select(:meanX, :meanY, :var = :b)
end
```

By default, `@chain` places the value of the 
previous expression into the first argument of the current
expression. The placeholder `_` is used to break that convention
and refer to the argument returned from the previous 
expression.

```julia
# Get the sum of all columns after 
# a few transformations
@chain df begin 
    @transform(:y = 10 .* :x)
    @subset(:a .> 2)
    @select(:a, :y, :x)
    reduce(+, eachcol(_))
end
```

`@chain` also provides the `@aside` macro-flag to perform operations
in the middle of a `@chain` block. 

```julia
@chain df begin 
    @transform :y = 10 .* :x
    @aside y_mean = mean(_.y) # From Chain.jl, not DataFramesMeta.jl
    @select :y_standardize = :y .- y_mean
end
```

```@contents
Pages = ["api/api.md"]
Depth = 3
```
