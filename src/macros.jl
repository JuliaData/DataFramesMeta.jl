##############################################################################
##
## @col
##
##############################################################################

"""
    @col(kw)

`@col` transforms an expression of the form `:z = :x + :y` into it's equivalent in
DataFrames's `source => fun => destination` syntax.

### Details

Parsing follows the same convention as other DataFramesMeta.jl macros, such as `@with`. All
terms in the expression that are `Symbol`s are treated as columns in the data frame, except
`Symbol`s wrapped in `^`. To use a variable representing a column name, wrap the variable
in `cols`.

`@col` constructs an anonymous function `fun` based on the given expression. It then creates
a `source => fun => destination` pair that is suitable for the `select`, `transform`, and
`combine` functions in DataFrames.jl.

### Examples

```julia
julia> @col :z = :x + :y
[:x, :y] => (##595 => :z)
```

In the above example, `##595` is an anonymous function equivalent to the following

```julia
(_x, _y) -> _x + _y
```

```jldoctest
julia> using DataFramesMeta;

julia> df = DataFrame(x = [1, 2], y = [3, 4]);

julia> import DataFramesMeta: @col;

julia> DataFrames.transform(df, @col :z = :x .* :y)
2×3 DataFrame
│ Row │ x     │ y     │ z     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 3     │ 3     │
│ 2   │ 2     │ 4     │ 8     │

```
"""
macro col(kw)
    esc(fun_to_vec(kw))
end

##############################################################################
##
## @byrow
##
##############################################################################
"""
    @byrow

Broadcast operations within DataFramesMeta.jl macros.

`@byrow` is not a "real" Julia macro but rather serves as a "flag"
to indicate that the anonymous function created by DataFramesMeta
to represent an operation should be applied "by-row".

If an expression starts with `@byrow`, either of the form `@byrow :y = f(:x)`
in transformations or `@byrow f(:x)` in `@orderby`, `@where`, and `@with`,
then the anonymous function created by DataFramesMeta is wrapped in the
`DataFrames.ByRow` function wrapper, which broadcasts the function so that it run on each row.

### Examples

```julia
julia> df = DataFrame(a = [1, 2, 3, 4], b = [5, 6, 7, 8]);

julia> @transform(df, @byrow :c = :a * :b)
4×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      5
   2 │     2      6     12
   3 │     3      7     21
   4 │     4      8     32

julia> @where(df, @byrow :a == 1 ? true : false)
1×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
```

To avoid writing `@byrow` multiple times when performing multiple
operations, it is allowed to use`@byrow` at the beginning of a block of
operations. All transformations in the block will operate by row.

```julia
julia> @where df @byrow begin
           :a > 1
           :b < 5
       end
1×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      4
```

### Comparison with `@eachrow`

To re-cap, the `@eachrow` macro roughly transforms

```julia
@eachrow df begin
    :a * :b
end
```

to

```julia
begin
    function tempfun(a, b)
        for i in eachindex(a)
            a[i] * b[i]
        end
    end
    tempfun(df.a, df.b)
    df
end
```

The function `*` is applied by-row. But the result of those operations
is not stored anywhere, as with `for`-loops in Base Julia.
Rather, `@eachrow` and `@eachrow!` return data frames.

Now consider `@byrow`. `@byrow` transforms

```julia
@with df @byrow begin
    :a * :b
end
```

to

```julia
tempfun(a, b) = a * b
tempfun.(df.a, df.b)
```

In contrast to `@eachrow`, `@with` combined with `@byrow` returns a vector of the
broadcasted multiplication and not a data frame.

Additionally, transformations applied using `@eachrow!` modify the input
data frame. On the contrary, `@byrow` does not update columns.

```julia
julia> df = DataFrame(a = [1, 2], b = [3, 4]);

julia> @with df @byrow begin
           :a = 500
       end
2-element Vector{Int64}:
 500
 500

julia> df
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4
```

### Comparison with `@.` and Base broadcasting

Base Julia provides the broadasting macro `@.` and in many cases `@.`
and `@byrow` will give equivalent results. But there are important
deviations in behavior. Consider the setup

```julia
df = DataFrame(a = [1, 2], b = [3, 4])
```

* Control flow. `@byrow` allows for operations of the form `if ... else`
  and `a ? b : c` to be applied by row. These expressions cannot be
  broadcasted in Base Julia. `@byrow` also allows for expressions of
  the form `a && b` and `a || b` to be applied by row, something that
  is not possible in Julia versions below 1.7.

```
julia> @with df @byrow begin
           if :a == 1
               5
           else
               10
           end
       end
2-element Vector{Int64}:
  5
 10

julia> @with df @. begin
           if :a == 1
               5
           else
               10
           end
       end # will error
```

* Broadcasting objects that are not columns. `@byrow` constructs an
  anonymous function which accepts only the columns of the input data frame
  and broadcasts that function. Consequently, it does not broadcast
  referenced objects which are not columns.

```julia
julia> df = DataFrame(a = [1, 2], b = [3, 4]);
julia> @with df @byrow :x + [5, 6]
```

  will error, because the `:x` in the above expression refers
  to a scalar `Int`, and you cannot do `1 + [5, 6]`.

  On the other hand

```julia
@with df @. :x + [5, 6]
```

  will succeed, as `df.x` is a 2-element vector as is `[5, 6]`.

  Because `ByRow` inside `transform` blocks does not internally
  use broadcasting in all circumstances, in the rare instance
  that a column in a data frame is a custom vector type that
  implements custom broadcasting, this custom behavior will
  not be called with `@byrow`.

* Broadcasting expensive calls. In Base Julia, broadcasting
  evaluates calls first and then broadcasts the result. Because
  `@byrow` constructs an anonymous function and evaluates
  that function for every row in the data frame, expensive functions
  will be evaluated many times.

```julia
julia> function expensive()
           sleep(.5)
           return 1
       end;

julia> @time @with df @byrow :a + expensive();
  1.037073 seconds (51.67 k allocations: 3.035 MiB, 3.19% compilation time)

julia> @time @with df :a .+ expensive();
  0.539900 seconds (110.67 k allocations: 6.525 MiB, 7.05% compilation time)

```

  This problem comes up when using the `@.` macro as well,
  but can easily be fixed with `\$`.

```julia
julia> @time @with df @. :a + expensive();
  1.036888 seconds (97.55 k allocations: 5.617 MiB, 3.20% compilation time)

julia> @time @with df @. :a + \$expensive();
  0.537961 seconds (110.68 k allocations: 6.525 MiB, 6.73% compilation time)
```

  No such solution currently exists with `@byrow`.
"""
macro byrow(args...)
    throw(ArgumentError("@byrow is deprecated outside of DataFramesMeta macros."))
end


##############################################################################
##
## @with
##
##############################################################################

function exec(df, p::Pair)
    cols = first(p)
    fun = last(p)
    fun(map(c -> DataFramesMeta.getsinglecolumn(df, c), cols)...)
end
exec(df, s::Union{Symbol, AbstractString}) = df[!, s]

getsinglecolumn(df, s::DataFrames.ColumnIndex) = df[!, s]
getsinglecolumn(df, s) = throw(ArgumentError("Only indexing with Symbols, strings and integers " *
    "is currently allowed with cols"))

function with_helper(d, body)
    # Make body an expression to force the
    # complicated method of fun_to_vec
    # in the case of QuoteNode
    t = fun_to_vec(Expr(:block, body); no_dest=true)
    :(DataFramesMeta.exec($d, $t))
end

"""
    @with(d, expr)

`@with` allows DataFrame columns keys to be referenced as symbols.

### Arguments

* `d` : an AbstractDataFrame type
* `expr` : the expression to evaluate in `d`

### Details

`@with` works by parsing the expression body for all columns indicated
by symbols (e.g. `:colA`). Then, a function is created that wraps the
body and passes the columns as function arguments. This function is
then called. Operations are efficient because:

- A pseudo-anonymous function is defined, so types are stable.
- Columns are passed as references, eliminating DataFrame indexing.

The following

```julia
@with(d, :a .+ :b .+ 1)
```

becomes

```julia
tempfun(a, b) = a .+ b .+ 1
tempfun(d[!, :a], d[!, :b])
```

If an expression is wrapped in `^(expr)`, `expr` gets passed through untouched.
If an expression is wrapped in  `cols(expr)`, the column is referenced by the
variable `expr` rather than a symbol.

If the expression provide to `@with` begins with `@byrow`, the function
created by the `@with` block is broadcasted along the columns of the
data frame.

### Examples

```jldoctest
julia> using DataFramesMeta

julia> y = 3;

julia> df = DataFrame(x = 1:3, y = [2, 1, 2]);

julia> x = [2, 1, 0];

julia> @with(df, :y .+ 1)
3-element Array{Int64,1}:
 3
 2
 3

julia> @with(df, :x + x)
3-element Array{Int64,1}:
 3
 3
 3

julia> @with df begin
            res = 0.0
            for i in 1:length(:x)
                res += :x[i] * :y[i]
            end
            res
        end
10.0

julia> @with(df, df[:x .> 1, ^(:y)]) # The ^ means leave the :y alone
2-element Array{Int64,1}:
 1
 2

julia> colref = :x;

julia> @with(df, :y + cols(colref)) # Equivalent to df[!, :y] + df[!, colref]
3-element Array{Int64,1}:
 3
 3
 5

julia> @with df @byrow :x * :y
3-element Vector{Int64}:
 2
 2
 6

```

!!! note
    `@with` creates a function, so the scope within `@with` is a local scope.
    Variables in the parent can be read. Writing to variables in the parent scope
    differs depending on the type of scope of the parent. If the parent scope is a
    global scope, then a variable cannot be assigned without using the `global` keyword.
    If the parent scope is a local scope (inside a function or let block for example),
    the `global` keyword is not needed to assign to that parent scope.
"""
macro with(d, body)
    esc(with_helper(d, body))
end


##############################################################################
##
## @subset and subset! - select row subsets
##
##############################################################################

function subset_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset($x, $(t...); skipmissing=true)
    end
end

function where_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset($x, $(t...); skipmissing=true)
    end
end

"""
    @subset(d, i...)

Select row subsets in `AbstractDataFrame`s and `GroupedDataFrame`s.

### Arguments

* `d` : an AbstractDataFrame or GroupedDataFrame
* `i...` : expression for selecting rows

Multiple `i` expressions are "and-ed" together.

If given a `GroupedDataFrame`, `@subset` applies transformations by
group, and returns a fresh `DataFrame` containing the rows
for which the generated values are all `true`.

Inputs to `@subset` can come in two formats: a `begin ... end` block, in which case each
line is a separate selector, or as multiple arguments.
For example the following two statements are equivalent:

```julia
@subset df begin
    :x .> 1
    :y .< 2
end
```

and

```
@subset(df, :x .> 1, :y .< 2)
```

!!! note
    `@subset` treats `missing` values as `false` when filtering rows.
    Unlike `DataFrames.subset` and other Boolean operations with
    `missing`, `@subset` will *not* error on missing values, and
    will only keep `true` values.

If an expression provided to `@subset` begins with `@byrow`, operations
are applied "by row" along the data frame. To avoid writing `@byrow` multiple
times, `@orderby` also allows `@byrow`to be placed at the beginning of a block of
operations. For example, the following two statements are equivalent.

```
@subset df @byrow begin
    :x > 1
    :y < 2
end
```

and

```
@subset df
    @byrow :x > 1
    @byrow :y < 2
end
```

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> df = DataFrame(x = 1:3, y = [2, 1, 2]);

julia> globalvar = [2, 1, 0];

julia> @subset(df, :x .> 1)
2×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     3      2

julia> @subset(df, :x .> globalvar)
2×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     3      2

julia> @subset df begin
    :x .> globalvar
    :y .== 3
end
0×2 DataFrame

julia> d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1,
                                    2, 1, 1, 2, 2, 2, 3, 1, 1, 2]);

julia> g = groupby(d, :x);

julia> @subset(g, :n .> mean(:n))
8×2 DataFrame
 Row │ n      x
     │ Int64  Int64
─────┼──────────────
   1 │    12      1
   2 │    13      1
   3 │    15      2
   4 │    16      2
   5 │    17      3
   6 │    18      1
   7 │    19      1
   8 │    20      2

julia> @subset g begin
           :n .> mean(:n)
           :n .< 20
       end
7×2 DataFrame
 Row │ n      x
     │ Int64  Int64
─────┼──────────────
   1 │    12      1
   2 │    13      1
   3 │    15      2
   4 │    16      2
   5 │    17      3
   6 │    18      1
   7 │    19      1

julia> d = DataFrame(a = [1, 2, missing], b = ["x", "y", missing]);

julia> @subset(d, :a .== 1)
1×2 DataFrame
│ Row │ a      │ b       │
│     │ Int64? │ String? │
├─────┼────────┼─────────┤
│ 1   │ 1      │ x       │
```
"""
macro subset(x, args...)
    esc(subset_helper(x, args...))
end

"""
    @where(x, args...)

Deprecated version of `@subset`, see `?@subset` for details.
"""
macro where(x, args...)
    @warn "`@where is deprecated, use `@subset`  with `@skipmissing` instead."
    esc(where_helper(x, args...))
end

function subset!_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset!($x, $(t...); skipmissing=true)
    end
end

"""
    @subset!(d, i...)

Select row subsets in `AbstractDataFrame`s and `GroupedDataFrame`s,
mutating the underlying data-frame in-place.

### Arguments

* `d` : an AbstractDataFrame or GroupedDataFrame
* `i...` : expression for selecting rows

Multiple `i` expressions are "and-ed" together.

If given a `GroupedDataFrame`, `@subset!` applies transformations by
group, and returns a fresh `DataFrame` containing the rows
for which the generated values are all `true`.

Inputs to `@subset!` can come in two formats: a `begin ... end` block, in which case each
line is a separate selector, or as multiple arguments.
For example the following two statements are equivalent:

```julia
@subset! df begin
    :x .> 1
    :y .< 2
end
```

and

```
@subset!(df, :x .> 1, :y .< 2)
```

!!! note
    `@subset!` treats `missing` values as `false` when filtering rows.
    Unlike `DataFrames.subset!` and other Boolean operations with
    `missing`, `@subset!` will *not* error on missing values, and
    will only keep `true` values.

If an expression provided to `@subset!` begins with `@byrow`, operations
are applied "by row" along the data frame. To avoid writing `@byrow` multiple
times, `@orderby` also allows `@byrow`to be placed at the beginning of a block of
operations. For example, the following two statements are equivalent.

```
@subset! df @byrow begin
    :x > 1
    :y < 2
end
```

and

```
@subset! df
    @byrow :x > 1
    @byrow :y < 2
end
```

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> df = DataFrame(x = 1:3, y = [2, 1, 2]);

julia> globalvar = [2, 1, 0];

julia> @subset!(df, :x .> 1)
2×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     3      2

julia> @subset!(df, :x .> globalvar)
2×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     3      2

julia> @subset! df begin
    :x .> globalvar
    :y .== 3
end
0×2 DataFrame

julia> d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1,
                                    2, 1, 1, 2, 2, 2, 3, 1, 1, 2]);

julia> g = groupby(d, :x);

julia> @subset!(g, :n .> mean(:n))
8×2 DataFrame
 Row │ n      x
     │ Int64  Int64
─────┼──────────────
   1 │    12      1
   2 │    13      1
   3 │    15      2
   4 │    16      2
   5 │    17      3
   6 │    18      1
   7 │    19      1
   8 │    20      2

julia> @subset! g begin
           :n .> mean(:n)
           :n .< 20
       end
7×2 DataFrame
 Row │ n      x
     │ Int64  Int64
─────┼──────────────
   1 │    12      1
   2 │    13      1
   3 │    15      2
   4 │    16      2
   5 │    17      3
   6 │    18      1
   7 │    19      1

julia> d = DataFrame(a = [1, 2, missing], b = ["x", "y", missing]);

julia> @subset!(d, :a .== 1)
1×2 DataFrame
│ Row │ a      │ b       │
│     │ Int64? │ String? │
├─────┼────────┼─────────┤
│ 1   │ 1      │ x       │
```
"""
macro subset!(x, args...)
    esc(subset!_helper(x, args...))
end


##############################################################################
##
## @orderby
##
##############################################################################

function orderby_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; gensym_names = true, outer_flags = outer_flags) for ex in exprs)
    quote
        $DataFramesMeta.orderby($x, $(t...))
    end
end

function orderby(x::AbstractDataFrame, @nospecialize(args...))
    t = DataFrames.select(x, args...; copycols = false)
    x[sortperm(t), :]
end

function orderby(x::GroupedDataFrame, @nospecialize(args...))
    throw(ArgumentError("@orderby with a GroupedDataFrame is reserved"))
end

function orderby(x::SubDataFrame, @nospecialize(args...))
    t = DataFrames.select(x, args...)
    x[sortperm(t), :]
end

"""
    @orderby(d, i...)

Sort rows by values in one of several columns or a transformation of columns.
Always returns a fresh `DataFrame`. Does not accept a `GroupedDataFrame`.

When given a `DataFrame`, `@orderby` applies the transformation
given by its arguments (but does not create new columns) and sorts
the given `DataFrame` on the result, returning a new `DataFrame`.

Inputs to `@orderby` can come in two formats: a `begin ... end` block, in which case each
line in the block is a separate ordering operation, and as mulitple
arguments. For example, the following two statements are equivalent:

```julia
@orderby df begin
    :x
    -:y
end
```

and

```
@orderby(df, :x, -:y)
```


### Arguments

* `d` : an AbstractDataFrame
* `i...` : expression for sorting

If an expression provided to `@orderby` begins with `@byrow`, operations
are applied "by row" along the data frame. To avoid writing `@byrow` multiple
times, `@orderby` also allows `@byrow`to be placed at the beginning of a block of
operations. For example, the following two statements are equivalent.

```
@orderby df @byrow begin
    :x^2
    :x^3
end
```

and

```
@orderby df
    @byrow :x^2
    @byrow :x^3
end
```

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> d = DataFrame(x = [3, 3, 3, 2, 1, 1, 1, 2, 1, 1], n = 1:10,
                     c = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]);

julia> @orderby(d, -1 .* :n)
10×3 DataFrame
 Row │ x      n      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     1     10  j
   2 │     1      9  i
   3 │     2      8  h
   4 │     1      7  g
   5 │     1      6  f
   6 │     1      5  e
   7 │     2      4  d
   8 │     3      3  c
   9 │     3      2  b
  10 │     3      1  a

julia> @orderby(d, sortperm(:c, rev = true))
10×3 DataFrame
 Row │ x      n      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     1     10  j
   2 │     1      9  i
   3 │     2      8  h
   4 │     1      7  g
   5 │     1      6  f
   6 │     1      5  e
   7 │     2      4  d
   8 │     3      3  c
   9 │     3      2  b
  10 │     3      1  a

julia> @orderby d begin
    :x
    abs.(:n .- mean(:n))
end
10×3 DataFrame
 Row │ x      n      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     1      5  e
   2 │     1      6  f
   3 │     1      7  g
   4 │     1      9  i
   5 │     1     10  j
   6 │     2      4  d
   7 │     2      8  h
   8 │     3      3  c
   9 │     3      2  b
  10 │     3      1  a

julia> @orderby d @byrow :x^2
10×3 DataFrame
 Row │ x      n      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     1      5  e
   2 │     1      6  f
   3 │     1      7  g
   4 │     1      9  i
   5 │     1     10  j
   6 │     2      4  d
   7 │     2      8  h
   8 │     3      1  a
   9 │     3      2  b
  10 │     3      3  c
```
"""
macro orderby(d, args...)
    esc(orderby_helper(d, args...))
end


##############################################################################
##
## transform & @transform
##
##############################################################################


function transform_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $DataFrames.transform($x, $(t...))
    end
end

"""
    @transform(d, i...)

Add additional columns or keys based on keyword arguments.

### Arguments

* `d` : an `AbstractDataFrame`, or `GroupedDataFrame`
* `i...` : keyword arguments defining new columns or keys

### Returns

* `::AbstractDataFrame` or `::GroupedDataFrame`

Inputs to `@transform` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, (`:y = f(:x)`), or as a series of
keyword arguments. For example, the following are
equivalent:

```julia
@transform df begin
    :a = :x
    :b = :y
end
```

and

```
@transform(df, :a = :x, :b = :y)
```

`@transform` uses the syntax `@byrow` to wrap transformations in
the `ByRow` function wrapper from DataFrames, apply a function row-wise,
similar to broadcasting. For example, the call

```
@transform(df, @byrow :y = :x == 1 ? true : false)
```

becomes

```
transform(df, :x => ByRow(x -> x == 1 ? true : false) => :y)
```

a transformation which cannot be conveniently expressed
using broadcasting.

To avoid writing `@byrow` multiple times when performing multiple
transformations by row, `@transform` allows `@byrow` at the
beginning of a block of transformations (i.e. `@byrow begin... end`).
All transformations in the block will operate by row.

### Examples

```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(A = 1:3, B = [2, 1, 2]);

julia> @transform df begin
           :a = 2 * :A
           :x = :A .+ :B
       end
3×4 DataFrame
 Row │ A      B      a      x
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      2      3
   2 │     2      1      4      3
   3 │     3      2      6      5

julia> @transform df @byrow :z = :A * :B
3×3 DataFrame
 Row │ A      B      z
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      2
   2 │     2      1      2
   3 │     3      2      6

julia> @transform df @byrow begin
           :x = :A * :B
           :y = :A == 1 ? 100 : 200
       end

3×4 DataFrame
 Row │ A      B      x      y
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      2    100
   2 │     2      1      2    200
   3 │     3      2      6    200

```
"""
macro transform(x, args...)
    esc(transform_helper(x, args...))
end

function rtransform_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    outer_flags[Symbol("@byrow")][] = true
    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $DataFrames.transform($x, $(t...))
    end
end

"""
    @rtransform(x, args...)

Row-wise version of `@transform`, see `? @transform` for details.
"""
macro rtransform(x, args...)
    esc(rtransform_helper(x, args...))
end

##############################################################################
##
## transform! & @transform!
##
##############################################################################


function transform!_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $DataFrames.transform!($x, $(t...))
    end
end

"""
    @transform!(d, i...)

Mutate `d` inplace to add additional columns or keys based on keyword arguments and return it.
No copies of existing columns are made.

### Arguments

* `d` : an `AbstractDataFrame`, or `GroupedDataFrame`
* `i...` : keyword arguments defining new columns or keys

### Returns

* `::DataFrame`

Inputs to `@transform!` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, (`:y = f(:x)`), or as a series of
keyword arguments. For example, the following are
equivalent:

```julia
@transform! df begin
    :a = :x
    :b = :y
end
```

and

```
@transform!(df, :a = :x, :b = :y)
```

`@transform!` uses the syntax `@byrow` to wrap transform!ations in
the `ByRow` function wrapper from DataFrames, apply a function row-wise,
similar to broadcasting. For example, the call

```
@transform!(df, @byrow :y = :x == 1 ? true : false)
```

becomes

```
transform!(df, :x => ByRow(x -> x == 1 ? true : false) => :y)
```

a transformation which cannot be conveniently expressed
using broadcasting.

To avoid writing `@byrow` multiple times when performing multiple
transform!ations by row, `@transform!` allows `@byrow` at the
beginning of a block of transform!ations (i.e. `@byrow begin... end`).
All transform!ations in the block will operate by row.

### Examples

```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(A = 1:3, B = [2, 1, 2]);

julia> df2 = @transform!(df, :a = 2 * :A, :x = :A .+ :B)
3×4 DataFrame
│ Row │ A     │ B     │ a     │ x     │
│     │ Int64 │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 2     │ 3     │
│ 2   │ 2     │ 1     │ 4     │ 3     │
│ 3   │ 3     │ 2     │ 6     │ 5     │

julia> df === df2
true
```
"""
macro transform!(x, args...)
    esc(transform!_helper(x, args...))
end


##############################################################################
##
## @select - select and transform columns
##
##############################################################################

function select_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $DataFrames.select($x, $(t...))
    end
end

"""
    @select(d, e...)

Select and transform columns.

### Arguments

* `d` : an `AbstractDataFrame` or `GroupedDataFrame`
* `e` :  keyword arguments specifying new columns in terms of existing columns
  or symbols to specify existing columns

### Returns

* `::AbstractDataFrame`

Inputs to `@select` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation or selector, or as a series of
arguments and keyword arguments. For example, the following are
equivalent:

```julia
@select df begin
    :x
    :y = :a .+ :b
end
```

and

```
@select(df, :x, :y = :a .+ :b)
```

`@select` uses the syntax `@byrow` to wrap transformations in
the `ByRow` function wrapper from DataFrames, apply a function row-wise,
similar to broadcasting. For example, the call

```
@select(df, @byrow :y = :x == 1 ? true : false)
```

becomes

```
select(df, :x => ByRow(x -> x == 1 ? true : false) => :y)
```

a transformation which cannot be conveniently expressed
using broadcasting.

To avoid writing `@byrow` multiple times when performing multiple
transformations by row, `@select` allows `@byrow` at the
beginning of a block of selectations (i.e. `@byrow begin... end`).
All transformations in the block will operate by row.

### Examples

```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(a = repeat(1:4, outer = 2), b = repeat(2:-1:1, outer = 4), c = 1:8);

julia> @select(df, :c, :a)
8×2 DataFrame
 Row │ c      a
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      1
   6 │     6      2
   7 │     7      3
   8 │     8      4

julia> @select df begin
           :c
           :x = :b + :c
       end
8×2 DataFrame
 Row │ c      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      3
   3 │     3      5
   4 │     4      5
   5 │     5      7
   6 │     6      7
   7 │     7      9
   8 │     8      9
```
"""
macro select(x, args...)
    esc(select_helper(x, args...))
end


##############################################################################
##
## @select! - in-place select and transform columns
##
##############################################################################

function select!_helper(x, args...)
    exprs, outer_flags = create_args_vector(args...)
    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $DataFrames.select!($x, $(t...))
    end
end

"""
    @select!(d, e...)

Mutate `d` in-place to retain only columns or transformations specified by `e` and return it. No copies of existing columns are made.

### Arguments

* `d` : an AbstractDataFrame
* `e` :  keyword arguments specifying new columns in terms of existing columns
  or symbols to specify existing columns

### Returns

* `::DataFrame`

Inputs to `@select!` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation or selector, or as a series of
arguments and keyword arguments. For example, the following are
equivalent:

`@select!` uses the syntax `@byrow` to wrap transformations in
the `ByRow` function wrapper from DataFrames, apply a function row-wise,
similar to broadcasting. For example, the call

```
@select!(df, @byrow :y = :x == 1 ? true : false)
```

becomes

```
select!(df, :x => ByRow(x -> x == 1 ? true : false) => :y)
```

a transformation which cannot be conveniently expressed
using broadcasting.

To avoid writing `@byrow` multiple times when performing multiple
transformations by row, `@select!` allows `@byrow` at the
beginning of a block of select!ations (i.e. `@byrow begin... end`).
All transformations in the block will operate by row.

### Examples

```jldoctest
julia> using DataFrames, DataFramesMeta

julia> df = DataFrame(a = repeat(1:4, outer = 2), b = repeat(2:-1:1, outer = 4), c = 1:8);

julia> df2 = @select!(df, :c, :a)
8×2 DataFrame
 Row │ c      a
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      1
   6 │     6      2
   7 │     7      3
   8 │     8      4

julia> df === df2
true

julia> df = DataFrame(a = repeat(1:4, outer = 2), b = repeat(2:-1:1, outer = 4), c = 1:8);

julia> df2 = @select! df begin
           :c
           :x = :b + :c
       end
8×2 DataFrame
 Row │ c      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      3
   3 │     3      5
   4 │     4      5
   5 │     5      7
   6 │     6      7
   7 │     7      9
   8 │     8      9

julia> df === df2
true
```
"""
macro select!(x, args...)
    esc(select!_helper(x, args...))
end


##############################################################################
##
## @combine - summarize a grouping operation
##
##############################################################################

function combine_helper(x, args...; deprecation_warning = false)
    deprecation_warning && @warn "`@based_on` is deprecated. Use `@combine` instead."

    exprs, outer_flags = create_args_vector(args...)

    fe = first(exprs)
    if length(exprs) == 1 &&
        !(fe isa QuoteNode || onearg(fe, :cols)) &&
        !(fe.head == :(=) || fe.head == :kw)

        @warn "Returning a Table object from @by and @combine now requires `cols(AsTable)` on the LHS."

        exprs = ((:(cols(AsTable) = $fe)),)
    end

    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)

    quote
        $DataFrames.combine($x, $(t...))
    end
end

"""
    @combine(x, args...)

Summarize a grouping operation

### Arguments

* `x` : a `GroupedDataFrame` or `AbstractDataFrame`
* `args...` : keyword arguments defining new columns

Inputs to `@combine` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, or as a series of keyword arguments.
For example, the following are equivalent:

```
@combine df begin
    :mx = mean(:x)
    :sx = std(:x)
end
```

and

```
@combine(df, :mx = mean(:x), :sx = std(:x))
```

### Examples

```jldoctest
julia> using DataFramesMeta

julia> d = DataFrame(
            n = 1:20,
            x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2]);

julia> g = groupby(d, :x);

julia> @combine(g, :nsum = sum(:n))
3×2 DataFrame
 Row │ x      nsum
     │ Int64  Int64
─────┼──────────────
   1 │     1     99
   2 │     2     84
   3 │     3     27

julia> @combine g begin
           :x2 = 2 * :x
           :nsum = sum(:n)
       end
20×3 DataFrame
 Row │ x      x2     nsum
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2     99
   2 │     1      2     99
   3 │     1      2     99
   4 │     1      2     99
   5 │     1      2     99
   6 │     1      2     99
   7 │     1      2     99
   8 │     1      2     99
   9 │     1      2     99
  10 │     2      4     84
  11 │     2      4     84
  12 │     2      4     84
  13 │     2      4     84
  14 │     2      4     84
  15 │     2      4     84
  16 │     3      6     27
  17 │     3      6     27
  18 │     3      6     27
  19 │     3      6     27
  20 │     3      6     27

```
"""
macro combine(x, args...)
    esc(combine_helper(x, args...))
end


"""
    @based_on(d, args...)

Deprecated version of `@combine`, see: [`@combine`](@ref)
"""
macro based_on(x, args...)
    esc(combine_helper(x, args...; deprecation_warning = true))
end

##############################################################################
##
## @by - grouping
##
##############################################################################

function by_helper(x, what, args...)
    # Only allow one argument when returning a Table object
    exprs, outer_flags = create_args_vector(args...)
    fe = first(exprs)
    if length(exprs) == 1 &&
        !(fe isa QuoteNode || onearg(fe, :cols)) &&
        !(fe.head == :(=) || fe.head == :kw)

        @warn "Returning a Table object from @by and @combine now requires `cols(AsTable)` on the LHS."

        exprs = ((:(cols(AsTable) = $fe)),)
    end

    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)

    quote
        $DataFrames.combine($groupby($x, $what), $(t...))
    end
end

"""
    @by(d::AbstractDataFrame, cols, e...)

Split-apply-combine in one step.

### Arguments

* `d` : an AbstractDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `e` :  keyword arguments specifying new columns in terms of column groupings

### Returns

* `::DataFrame`

Transformation inputs to `@by` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, or as a series of keyword arguments.
For example, the following are equivalent:

```
@by df :g begin
    :mx = mean(:x)
    :sx = std(:x)
end
```

and

```
@by(df, :g, mx = mean(:x), sx = std(:x))
```

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> df = DataFrame(
            a = repeat(1:4, outer = 2),
            b = repeat(2:-1:1, outer = 4),
            c = 1:8);

julia> @by(df, :a, :d = sum(:c))
4×2 DataFrame
 Row │ a      d
     │ Int64  Int64
─────┼──────────────
   1 │     1      6
   2 │     2      8
   3 │     3     10
   4 │     4     12

julia> @by df :a begin
           :d = 2 * :c
       end
8×2 DataFrame
 Row │ a      d
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     1     10
   3 │     2      4
   4 │     2     12
   5 │     3      6
   6 │     3     14
   7 │     4      8
   8 │     4     16

julia> @by(df, :a, :c_sum = sum(:c), :c_mean = mean(:c))
4×3 DataFrame
 Row │ a      c_sum  c_mean
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      6      3.0
   2 │     2      8      4.0
   3 │     3     10      5.0
   4 │     4     12      6.0

julia> @by df :a begin
           :c = :c
           :c_mean = mean(:c)
       end
8×3 DataFrame
 Row │ a      c      c_mean
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      1      3.0
   2 │     1      5      3.0
   3 │     2      2      4.0
   4 │     2      6      4.0
   5 │     3      3      5.0
   6 │     3      7      5.0
   7 │     4      4      6.0
   8 │     4      8      6.0

```
"""
macro by(x, what, args...)
    esc(by_helper(x, what, args...))
end
