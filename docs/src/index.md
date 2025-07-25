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
* `@rename` and `@rename!` for renaming columns
* `@groupby` for grouping data
* `@by`, for grouping and combining a data frame in a single step
* `@with`, for working with the columns of a data frame with high performance and 
  convenient syntax
* `@eachrow` and `@eachrow!` for looping through rows in data frame, again with high performance and 
  convenient syntax. 
* `@byrow` for applying functions to each row of a data frame (only supported inside other macros).
* `@passmissing` for propagating missing values inside row-wise DataFramesMeta.jl transformations.
* `@astable` to create multiple columns within a single transformation.
* `@chain`, from [Chain.jl](https://github.com/jkrumbiegel/Chain.jl) for piping the above macros together, similar to [magrittr](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html)'s
  `%>%` in R. 
* `@label!` and `@note!` for attaching metadata to columns. 

See below the convenience of DataFramesMeta compared to DataFrames.

```julia
df = DataFrame(a = [1, 2], b = [3, 4]);

# With DataFrames
transform(df, [:a, :b] => ((x, y) -> x + y) => :c)

# With DataFramesMeta
@transform(df, :c = :a + :b)

# With DataFrames
subset(df, :a => ByRow(==(2)))

# With DataFramesMeta
@rsubset(df, :a == 2)
```

To reference columns inside DataFramesMeta macros, use `Symbol`s. For example, use `:x`
to refer to the column `df.x`. To use a variable `varname` representing a `Symbol` to refer to 
a column, use the syntax `$varname`. 

Use `passmissing`  to propagate `missing` values more easily. See `?passmissing` for 
details. `passmissing` is defined in [Missings.jl](https://github.com/JuliaData/Missings.jl)
but exported by DataFramesMeta for convenience. 

# Provided macros

## `@select` and `@select!`

Column selections and transformations. Only newly created columns are kept. 
Operates on both a `DataFrame` and a `GroupedDataFrame`. Transformations are 
called with the keyword-like syntax `:y = f(:x)`. 

`@select` returns a new data frame with newly allocated columns, while `@select!`
mutates the original data frame and returns it.

When given a `GroupedDataFrame`, performs a transformation by group and then 
if necessary repeats the result to have as many rows as the input 
data frame. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = @groupby(df, :x);
@select(df, :x, :y)
@select(df, :x2 = 2 * :x, :y)
@select(gd, :x2 = 2 .* :y .* first(:y))
@select!(df, :x, :y)
@select!(df, :x = 2 * :x, :y)
@select!(gd, :y = 2 .* :y .* first(:y))
```

To select or de-select multiple columns, use `Not`, `Between`, `All`, and `Cols`. 
These multi-column selectors are all re-exported from DataFrames.jl. 

```julia
@select df Not(:x)
@select df Between(:x, :y)
@select df All()
@select df Cols(r"x") # Regular expressions.
```

## `@transform` and `@transform!`

Add additional columns based on keyword-like arguments. Operates on both a 
`DataFrame` and a `GroupedDataFrame`. Transformations are 
called with the keyword-like syntax `:y = f(:x)`. 

`@transform` returns a new data frame with newly allocated columns, while `@transform!`
mutates the original data frame and returns it.

When given a `GroupedDataFrame`, performs a transformation by group and then 
if necessary repeats the result to have as many rows as the input 
data frame. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = @groupby(df, :x);
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
gd = @groupby(df, :x);
outside_var = 1;
@subset(df, :x .> 1)
@subset(df, :x .> outside_var)
@subset(df, :x .> outside_var, :y .< 102)  # the two expressions are "and-ed"
@subset(df, in.(:y, Ref([101, 102]))) # pick rows with values found in a reference list
@rsubset(df, :y in [101, 102]) # the same with @rsubset - explained below; broadcasting is not needed
@subset(gd, :x .> mean(:x))
```

## `@combine`

Summarize, or collapse, a grouped data frame by performing transformations at the group level and 
collecting the result into a single data frame. Also works on a `DataFrame`, which 
acts like a `GroupedDataFrame` with one group. 

Like `@select` and `@transform`, transformations are called with the keyword-like 
syntax `:y = f(:x)`. 

To group data together into a `GroupedDataFrame`, use `@groupby`, a short-hand for
the DataFrames.jl function `groupby`.

Examples:

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = @groupby(df, :x);
@combine(gd, :x2 = sum(:y))
@combine(gd, :x2 = :y .- sum(:y))
@combine(gd, $AsTable = (n1 = sum(:y), n2 = first(:y)))
```

The last example tells the underlying DataFrames.jl function `combine` 
that the output should be a "Table" in the [Tables.jl](https://tables.juliadata.org/stable/) 
sense. For more information, see the documentation for `DataFrames.combine` and 
the [section below](@ref dollar) on escaping column identifiers with `$`. 

`@combine` requires a `DataFrame` or `GroupedDataFrame` as the first argument. This is
unlike `combine` from DataFrames.jl, which can take a function as the first argument
and a `GroupedDataFrame` as the second argument.
For instance, `@combine((a = sum(:x), b = sum(:y)), gd)` will fail. 
The following, however, will work.

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
gd = groupby(df, :x);
@combine(gd, $AsTable = (a = sum(:x), b = sum(:y)))
```

### `@by` 

Perform the grouping and combining operations in one step with `@by`

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
@by df :x begin
    :y_sum = sum(:y)
end
```

## `@orderby`

Sort rows in a `DataFrame` by values in one of several columns or a 
transformation of columns. Only operates on `DataFrame`s and not `GroupedDataFrame`s. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
@orderby(df, -1 .* :x)
@orderby(df, :x, :y .- mean(:y))
```

## `@rename`

Rename columns in a data frame using the keyword argument-like syntax `:new = :old`. Like other macros, `@rename` can be used in both multi-argument and "block" format. 

```julia
df = DataFrame(x = [1, 1, 2, 2], y = [1, 2, 101, 102]);
@rename df :x_new = :x
@rename(df, :x_new = :x)
@rename df $"Name with spaces" = :y
@rename df begin 
    :x_new = :x
    :y_new = :y
end
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

    ```julia
    function data_transform(df; returnearly = true)
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
df = DataFrame(A = 1:3, B = [2, 1, 2], C = [-4,2,1])
let x = 0.0
    @eachrow df begin
        if :A < :B
            x += :A * :C
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
transform(df, :x => ByRow(x -> x == 1 ? true : false) => :y)
```

The following macros accept `@byrow`:

* `@transform` and `@transform!`, `@select`, `@select!`, and `@combine`. 
  `@byrow` can be used in the left hand side of expressions, e.g.
  `@select(df, @byrow z = :x * :y)`. 
* `@subset`, `@subset!` and `@orderby`, with syntax of the form `@subset(df, @byrow :x > :y)`
* `@with`, where the anonymous function created by `@with` is wrapped in
  `ByRow`, as in `@with(df, @byrow :x * :y)`.

To avoid writing `@byrow` multiple times when performing multiple
operations, it is allowed to use `@byrow` at the beginning of a block of 
operations. All transformations in the block will operate by row.

```julia-repl
julia> df = DataFrame(a = [1, 2], b = [3, 4]);

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
example the result of `@transform(df, @byrow :y = f(:x))` and 
`@transform(@groupby(df, :g), @byrow :y = f(:x))` is the same.

## Propagating missing values with `@passmissing`

Many Julia functions do not automatically propagate missing values. For instance, 
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

```julia
transform(df, [:a, :b] => ByRow(passmissing(f)) => :c)
```

See more examples below.

```julia-repl
julia> no_missing(x::Int, y::Int) = x + y;

julia> df = DataFrame(a = [1, 2, missing], b = [4, 5, 6])
3×2 DataFrame
 Row │ a        b
     │ Int64?   Int64
─────┼────────────────
   1 │       1      4
   2 │       2      5
   3 │ missing      6

julia> @transform df @passmissing @byrow :c = no_missing(:a, :b)
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

julia> @rtransform df @passmissing :x = parse(Int, :x_str)
3×2 DataFrame
 Row │ x_str    x
     │ String?  Int64?
─────┼──────────────────
   1 │ 1              1
   2 │ 2              2
   3 │ missing  missing
```

## Passing keyword arguments to underlying DataFrames.jl functions

All DataFramesMeta.jl macros allow passing of keyword arguments to their DataFrames.jl
function equivalents. The table below describes the correspondence between DataFramesMeta.jl
macros and the function that is actually called by the macro. 

| Macro | Base DataFrames.jl function called |
|-------|---------------------------|
| `@subset` | `subset` |
| `@subset!` | `subset!` |
| `@rsubset` | `subset` |
| `@rsubset!` | `subset!` |
| `@orderby` | None (no keyword arguments supported) |
| `@rorderby` | None (no keyword arguments supported) |
| `@by` | `combine` |
| `@combine` | `combine` |
| `@transform` | `transform` |
| `@transform!` | `transform!` |
| `@rtransform` | `transform` |
| `@rtransform!` | `transform!` |
| `@select` | `select` |
| `@select!` | `select!` |
| `@rselect` | `select` |
| `@rselect!` | `select!` |

This can be done in two ways. When inputs are given as multiple 
arguments, they are added at the end after a semi-colon `;`, as in

```julia-repl
julia> df = DataFrame(x = [1, 1, 2, 2], b = [5, 6, 7, 8]);

julia> @rsubset(df, :x == 1 ; view = true)
2×2 SubDataFrame
 Row │ x      b     
     │ Int64  Int64 
─────┼──────────────
   1 │     1      5
   2 │     1      6

```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `subset` function.

```julia-repl
julia> df = DataFrame(x = [1, 1, 2, 2], b = [5, 6, 7, 8]);

julia> @rsubset df begin
           :x == 1
           @kwarg view = true
       end
2×2 SubDataFrame
 Row │ x      b     
     │ Int64  Int64 
─────┼──────────────
   1 │     1      5
   2 │     1      6
```

Just as with Julia functions, it is possible to pass keyword arguments as `Pair`s 
programatically to DataFramesMeta.jl macros. 

```julia-repl
julia> df = DataFrame(x = [1, 1, 2, 2], b = [5, 6, 7, 8]);

julia> my_kwargs = [:view => true, :skipmissing => false];

julia> @rsubset(df, :x == 1; my_kwargs...)
2×2 SubDataFrame
 Row │ x      b     
     │ Int64  Int64 
─────┼──────────────
   1 │     1      5
   2 │     1      6

julia> @rsubset df begin 
           :x == 1
           @kwarg my_kwargs...
       end
2×2 SubDataFrame
 Row │ x      b     
     │ Int64  Int64 
─────┼──────────────
   1 │     1      5
   2 │     1      6
```

## Creating multiple columns at once with `@astable`

Often new variables may depend on the same intermediate calculations. `@astable` makes it easy to create multiple
new variables in the same operation, yet have them share
information. 

In a single block, all assignments of the form `:y = f(:x)` 
or `$y = f(:x)` at the top-level generate new columns. In the second form, `y`
must be a string or `Symbol`. 

```julia-repl
julia> df = DataFrame(a = [1, 2, 3], b = [400, 500, 600]);

julia> @transform df @astable begin 
           ex = extrema(:b)
           :b_first = :b .- first(ex)
           :b_last = :b .- last(ex)
       end
3×4 DataFrame
 Row │ a      b      b_first  b_last 
     │ Int64  Int64  Int64    Int64  
─────┼───────────────────────────────
   1 │     1    400        0    -200
   2 │     2    500      100    -100
   3 │     3    600      200       0
```

## Operations with multiple columns at once using `AsTable` inside operations

In operations, it is also allowed to use `AsTable(cols)` to work with
multiple columns at once, where the columns are grouped together in a
`NamedTuple`. When `AsTable(cols)` appears in a operation, no
other columns may be referenced in the block.

`AsTable` on the right-hand side also allows the use of the special
column selectors `Not`, `Between`, and regular expressions, as well
as working with lists of variables programmatically. 

For example, consider a collection of column names `vars`, such that

```julia
df = DataFrame(a = [11, 14], b = [17, 10], c = [12, 5]);
vars = ["a", "b"];
```

To make a new column which is the sum of `vars`, write

```julia-repl
julia> @rtransform df :y = sum(AsTable(vars))
2×4 DataFrame
 Row │ a      b      c      y     
     │ Int64  Int64  Int64  Int64 
─────┼────────────────────────────
   1 │    11     17     12     28
   2 │    14     10      5     24
```

Of course, you can also use `AsTable` on the right-hand side using `Symbol`s as column selectors

```julia-repl
julia> @rtransform df :y = sum(AsTable([:a, :b]))
2×4 DataFrame
 Row │ a      b      c      y     
     │ Int64  Int64  Int64  Int64 
─────┼────────────────────────────
   1 │    11     17     12     28
   2 │    14     10      5     24
```

`AsTable` on the right-hand side also allows operations which can use the names of the variables. 

```julia-repl
julia> function fun_with_new_name(x::NamedTuple)
           nms = string.(propertynames(x))
           new_name = Symbol(join(nms, "_"), "_sum")
           s = sum(x)
           (; new_name => s)
       end

julia> @rtransform df $AsTable = fun_with_new_name(AsTable([:a, :b]))
2×4 DataFrame
 Row │ a      b      c      a_b_sum 
     │ Int64  Int64  Int64  Int64   
─────┼──────────────────────────────
   1 │    11     17     12       28
   2 │    14     10      5       24
```

To subset all rows where the sum is greater than `25`, write

```julia-repl
julia> @rsubset df sum(AsTable(vars)) > 25
1×3 DataFrame
 Row │ a      b      c     
     │ Int64  Int64  Int64 
─────┼─────────────────────
   1 │    11     17     12
```

To understand the how this works, recall that DataFrames.jl allows for
`AsTable(cols)` to be a `source` in a `source => fun => dest` mini-language
expression. As a consequence, the transformation call

```julia
:y = f(AsTable(cols)) 
```

becomes

```julia
AsTable(cols) => f => :y
```

Note that DataFrames does *not* allow `source => fun => dest` commands 
to be of the form 

```julia
[AsTable(cols), :x] => f => :y
```

As a consequence, DataFramesMeta.jl does not allow any other column selectors to appear 
inside the expression. The command

```julia
:y = sum(AsTable(cols)) + :d
```

will fail. 

Finally, note that everything inside `AsTable` is escaped by default.
There is no ned to use `$` inside `AsTable` on the right-hand side.
For example

```julia
:y = first(AsTable("a"))
```

will work as expected.  


## AsTable and `@astable`, explained

At this point we have seen `AsTable` appear in three places:

1. `AsTable` on the left-hand side of transformations: `$AsTable = f(:a, :b)`
2. The macro-flag `@astable` within the transformation. 
3. `AsTable(cols)` on the right-hand side for multi-column transformations. 

The differences between the three is summarized below

| Operation         | Purpose                                                                             | Notes |
|-------------------|-------------------------------------------------------------------------------------|-------|
| `$AsTable` on LHS | Create multiple columns at once, whose column names are only known programmatically |  Requires escaping with `$` until deprecation period ends for unquoted column names on LHS. |
| `@astable`        | Create multiple columns at once where number of columns is known in advance         | |
| `AsTable` on RHS  | Work with multiple columns at once                                                  | Requires input columns, unlike on LHS |

## [Working with column names programmatically with `$`](@id dollar)

DataFramesMeta provides the special syntax `$` for referring to 
columns in a data frame via a `Symbol`, string, or column position as either a literal or a variable. 

```julia
df = DataFrame(A = 1:3, :B = [2, 1, 2])

nameA = :A
df2 = @transform(df, :C = :B - $nameA)

nameA_string = "A"
df3 = @transform(df, :C = :B - $nameA_string)

nameB = "B"
df4 = @eachrow df begin 
    :A = $nameB
end
```

`$` can also be used to create new columns in a data frame. 

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
of other types. This means `@transform(df, :y = :A + $2)`, attempting to add the columns 
`df[!, :A]` and `df[!, 2]`, will fail. This is because in DataFrames, the command 

```julia
transform(df, [:A, 2] => (+) => :y)
``` 

will fail, as DataFrames requires the "source" column identifiers in a 
`source => fun => dest` pair to all have the same type. DataFramesMeta adds one exception
to this rule. `Symbol`s and strings are allowed to be mixed inside DataFramesMeta macros. 
Consequently, 

```julia
@transform(df, :y = :A + $"B")
```

will not error even though 

```julia
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

To reference columns with more complicated expressions, you must wrap column references in parentheses. 

```julia
@transform df :a + $("a column name" * " in two parts")
@transform df :a + $(get_column_name(x))
```

## Using `src => fun => dest` calls using `$`

If an argument is entirely wrapped in `$()`, the result bypasses the anonymous function 
creation of DataFramesMeta.jl and is passed to the underling DataFrames.jl function 
directly. Importantly, this allows for `src => fun => dest` calls from the DataFrames.jl 
"mini-language" directly. One example where this is useful is calling multiple functions across multiple input parameters. For instance, the `Pair`

```julia
[:a, :b] .=> [sum mean]
```

takes the `sum` and `mean` of both columns `:a` and `:b` separately. It is not possible to express this with DataFramesMeta.jl. But the operation can easily be performed with `$`

```julia-repl
julia> using Statistics

julia> df = DataFrame(a = [1, 2], b = [30, 40]);

julia> @transform df $([:a, :b] .=> [sum mean])
2×6 DataFrame
 Row │ a      b      a_sum  b_sum  a_mean   b_mean  
     │ Int64  Int64  Int64  Int64  Float64  Float64 
─────┼──────────────────────────────────────────────
   1 │     1     30      3     70      1.5     35.0
   2 │     2     40      3     70      1.5     35.0
```

## Multi-argument column selection 

To refer to multiple columns in DataFrames.jl, one can write

```julia
select(df, [:a, :b])
```

which selects the columns `:a` and `:b` in the data frame. We can generate this command in DataFramesMeta.jl with

```julia
@select df $[:a, :b]
```

Similarly, to select all columns beginning with the letter `"a"`, wrap a regular expression in `$()`. As mentioned above, because the regex is a complicated syntax, we need to wrap it in parentheses, so that

```julia
@select df $(r"^a")
```

will construct the command `select(df, r"^a")`. 

Multi-argument selectors *may only* be used when an entire argument is wrapped in `$()`. For example

```julia
@select df :y = f($[:a, :b])
```

will fail. 

Not all functions in DataFrames.jl allow for multi-column selectors, so detailed knowledge of the underlying functions in DataFrames.jl may be required. For example, the call 

```julia
subset(df, [:a, :b])
```

will fail in DataFrames.jl, because `DataFrames.subset` does not support vectors of column names. Likewise, `@subset df $[:a, :b]` will fail. The macros which support multi-column selectors are 

* `@select`
* `@transform` (multi-argument selectors have no effect)
* `@combine`
* `@by`


Since arguments wrapped entirely in `$()` get passed directly to underlying DataFrames.jl functions, this allows the use of the DataFrames.jl "mini-language" consisting of `src => fun => dest` pairs inside DataFramesMeta.jl macros. For example, you can do the following:

```julia-repl
julia> df = DataFrame(a = [1, 2], b = [3, 4]);

julia> my_transformation = :a => (t -> t .+ 100) => :c;

julia> @transform df begin 
           $my_transformation
           :d = :b .+ 200
       end
2×4 DataFrame
 Row │ a      b      c      d     
     │ Int64  Int64  Int64  Int64 
─────┼────────────────────────────
   1 │     1      3    101    203
   2 │     2      4    102    204
```

or with `@subset`

```julia
julia> @subset df $(:a => t -> t .>= 2)
1×2 DataFrame
 Row │ a      b     
     │ Int64  Int64 
─────┼──────────────
   1 │     2      4
```

!!! warning
    The macros `@orderby` and `@with` do not transparently call underlying DataFrames.jl functions. Escaping entire transformations should be considered unstable and may change in future versions.

!!! warning
    Row-wise macros such as `@rtransform` and `@rsubset` will not automatically wrap functions in `src => fun => dest` in `ByRow`. 

In summary

* All arguments that are not *entirely* escaped with `$` or `$()` construct anonymous functions. Inside these expressions only single-column selectors are allowed. This includes
    * `Symbol`s, i.e. `:x` and `:y`
    * Strings, escaped with `$`, i.e. `$"A string"` or `$("A string with many" * "parts")`
    * Integers, escaped with `$`, i.e. `$1`
    * Any single-column variable representing one of the above, escaped with `$`, i.e. `$x`
  
  In transformation operations, i.e. `@transform :y = f(:x)`, the same rules on the right hand side also apply to the left hand side. For example, `@transform $"y" = f(:x)` will work. 

* Arguments wrapped entirely in `$` or `$()` are passed directly to the underlying DataFrames.jl functions. Because of this, *in addition to* the single-column selectors listed above, multi-argument selectors are allowed. These include, but are not limited to
    * Vectors of `Symbol`s, `$[:x, :y]`, strings, `$["x", "y"]`, or integers `$[1, 2]`
    * Regular expressions, `$(r"^a")`
    * Filtering column selectors, such as `$(Not(:x))` and `$(Between(:a, :z))`

    The macros `@with`, `@subset`, and `@orderby` do not support multi-column selectors. 

* Advanced users of DataFramesMeta.jl and DataFrames.jl may wrap an argument entirely in `$()` to pass `src => fun => dest` pairs directly to DataFrames.jl functions. However this is discouraged and it's behavior may change in future versions. 

## Working with `Symbol`s without referring to columns

To refer to `Symbol`s without aliasing the column in a data frame, use `^`. 

```julia
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

| Julia        | dplyr            | LINQ         |
|--------------|------------------|--------------|
| `@subset`    | `filter`         | `Where`      |
| `@transform` | `mutate`         | `Select` (?) |
| `@by`        |                  | `GroupBy`    |
| `@groupby`   | `group_by`       | `GroupBy`    |
| `@combine`   | `summarise`/`do` |              |
| `@orderby`   | `arrange`        | `OrderBy`    |
| `@select`    | `select`         | `Select`     |


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

## Attaching variable labels and notes

A widely used and appreciated feature of the Stata data analysis
programming language is it's tools for column-level metadata in the
form of labels and notes. Like Stata, Julia's data ecosystem implements a common 
API for keeping track of information associated with columns. DataFramesMeta.jl 
implements the `@label!` and `@note!` macros to attach information to columns. 

DataFramesMeta.jl also provides two convenience functions 
for examining metadata, `printlabels` and `printnotes`.

### `@label!`: For short column labels 

Use `@label!` to attach short-but-informative labels to columns. For example,
a variable `:wage` might be given the label `"Wage (2015 USD)"`. 

```julia
df = DataFrame(wage = [16, 25, 14, 23]);
@label! df :wage = "Wage (2015 USD)"
``` 

View the labels with `printlabels(df)`

```julia-repl
julia> printlabels(df)
┌────────┬─────────────────┐
│ Column │           Label │
├────────┼─────────────────┤
│   wage │ Wage (2015 USD) │
└────────┴─────────────────┘
```

You can access labels via the `label` function defined in TablesMetaDataTools.jl

```julia-repl
julia> label(df, :wage)
"Wage (2015 USD)"
```

### `@note!`: For longer column notes

While labels are useful for pretty printing and clarification of short variable
names, notes are used to give more in depth information and describe the data 
cleaning process. Unlike labels, notes can be stacked on to one another. 

Consider the cleaning process for wages, starting with the data frame

```julia-repl
julia> df = DataFrame(wage = [-99, 16, 14, 23, 5000])
5×1 DataFrame
 Row │ wage  
     │ Int64 
─────┼───────
   1 │   -99
   2 │    16
   3 │    14
   4 │    23
   5 │  5000
```

When data cleaning you might want to do the following:

1. Record the source of the data

   ```julia
   @note! df :wage = "Hourly wage from 2015 American Community Survey (ACS)"
   ```

2. Fix coded wages. In this example, `-99` corresponds to "no job"

   ```julia
   @rtransform! df :wage = :wage == -99 ? 0 : :wage
   @note! df :wage = "Individuals with no job are recorded as 0 wage"
   ```

We use `printnotes` to see the notes for columns. 

```julia-repl
julia> printnotes(df)
Column: wage
────────────
Hourly wage from 2015 American Community Survey (ACS)
Individuals with no job are recorded as 0 wage
```

You can access the note via the `note` function. 

```julia-repl
julia> note(df, :wage)
"Hourly wage from 2015 American Community Survey (ACS)\nIndividuals with no job are recorded as 0 wage"
```

To remove all notes from a column, run

```julia
note!(df, :wage, ""; append = false)
```

### Printing metadata

#### `printlabels`: For printing labels

Use `printlabels` to print the labels of columns in a data frame. The optional
argument `cols` determines which columns to print, while the keyword
argument `unlabelled` controls whether to print columns without user-defined labels. 

```julia-repl
julia> df = DataFrame(wage = [12], age = [23]);

julia> @label! df :wage = "Hourly wage (2015 USD)";

julia> printlabels(df)
┌────────┬────────────────────────┐
│ Column │                  Label │
├────────┼────────────────────────┤
│   wage │ Hourly wage (2015 USD) │
│    age │                    age │
└────────┴────────────────────────┘

julia> printlabels(df, [:wage, :age]; unlabelled = false)
┌────────┬────────────────────────┐
│ Column │                  Label │
├────────┼────────────────────────┤
│   wage │ Hourly wage (2015 USD) │
└────────┴────────────────────────┘
```

#### `printnotes`: For printing notes

Use `printnotes` to print the notes of columns in a data frame. The optional
argument `cols` determines which columns to print, while the keyword
argument `unnoted` controls whether to print columns without user-defined notes. 

```julia-repl
julia> df = DataFrame(wage = [12], age = [23]);

julia> @label! df :age = "Age (years)";

julia> @note! df :wage = "Derived from American Community Survey";

julia> @note! df :wage = "Missing values imputed as 0 wage";

julia> @label! df :wage = "Hourly wage (2015 USD)";

julia> printnotes(df)
Column: wage
────────────
Label: Hourly wage (2015 USD)
Derived from American Community Survey
Missing values imputed as 0 wage

Column: age
───────────
Label: Age (years)

```
