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
in `$DOLLAR`.

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
 Row │ x      y      z
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      3      3
   2 │     2      4      8

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
in transformations or `@byrow f(:x)` in `@orderby`, `@subset`, and `@with`,
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

julia> @subset(df, @byrow :a == 1 ? true : false)
1×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
```

To avoid writing `@byrow` multiple times when performing multiple
operations, it is allowed to use `@byrow` at the beginning of a block of
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

Base Julia provides the broadcasting macro `@.` and in many cases `@.`
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
  but can easily be fixed with `$DOLLAR`. Because `$DOLLAR` is currently
  reserved for escaping column references, no solution currently exists with
  `@byrow` or in DataFramesMeta.jl at large. The best solution is simply

```
@with df begin
    x = expensive()
    :a + x
end
```

"""
macro byrow(args...)
    throw(ArgumentError("@byrow is deprecated outside of DataFramesMeta macros."))
end

"""
    @passmissing(args...)

Propagate missing values inside DataFramesMeta.jl macros.


`@passmissing` is not a "real" Julia macro but rather serves as a "flag"
to indicate that the anonymous function created by DataFramesMeta.jl
to represent an operation should be wrapped in `passmissing` from Missings.jl.

`@passmissing` can only be combined with `@byrow` or the row-wise versions of macros
such as `@rtransform` and `@rselect`, etc. If any of the arguments passed
to the row-wise anonymous function created by DataFramesMeta.jl with `@byrow`, the
result will automatically be `missing`.

In the below example, `@transform` would throw an error without the `@passmissing`
flag.

`@passmissing` is especially useful for functions which operate on strings, such as
`parse`.

### Examples

```
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
"""
macro passmissing(args...)
    throw(ArgumentError("@passmissing only works inside DataFramesMeta macros."))
end

const ASTABLE_MACRO_FLAG_DOCS = """
    Transformations can also use the macro-flag [`@astable`](@ref) for creating multiple
    new columns at once and letting transformations share the same name-space.
    See `? @astable` for more details.
    """

"""
    @astable(args...)

Return a `NamedTuple` from a single transformation inside the DataFramesMeta.jl
macros, `@select`, `@transform`, and their mutating and row-wise equivalents.

`@astable` acts on a single block. It works through all top-level expressions
and collects all such expressions of the form `:y = ...` or `$(DOLLAR)y = ...`, i.e. assignments to a
`Symbol` or an escaped column identifier, which is a syntax error outside of
DataFramesMeta.jl macros. At the end of the expression, all assignments are collected
into a `NamedTuple` to be used with the `AsTable` destination in the DataFrames.jl
transformation mini-language.

Concretely, the expressions

```
df = DataFrame(a = 1)

@rtransform df @astable begin
    :x = 1
    y = 50
    :z = :x + y + :a
end
```

become the pair

```
function f(a)
    x_t = 1
    y = 50
    z_t = x_t + y + a

    (; x = x_t, z = z_t)
end

transform(df, [:a] => ByRow(f) => AsTable)
```

`@astable` has two major advantages at the cost of increasing complexity.
First, `@astable` makes it easy to create multiple columns from a single
transformation, which share a scope. For example, `@astable` allows
for the following (where `:x` and `:x_2` exist in the data frame already).

```
@transform df @astable begin
    m = mean(:x)
    :x_demeaned = :x .- m
    :x2_demeaned = :x2 .- m
end
```

The creation of `:x_demeaned` and `:x2_demeaned` both share the variable `m`,
which does not need to be calculated twice.

Second, `@astable` is useful when performing intermediate calculations
and storing their results in new columns. For example, the following fails.

```
@rtransform df begin
    :new_col_1 = :x + :y
    :new_col_2 = :new_col_1 + :z
end
```

This because DataFrames.jl does not guarantee sequential evaluation of
transformations. `@astable` solves this problem

@rtransform df @astable begin
    :new_col_1 = :x + :y
    :new_col_2 = :new_col_1 + :z
end

Column assignment in `@astable` follows similar rules as
column assignment in other DataFramesMeta.jl macros. The left-
-hand-side of a column assignment can be either a `Symbol` or any
expression which evaluates to a `Symbol` or `AbstractString`. For example
`:y = ...`, and `$(DOLLAR)y = ...` are both valid ways of assigning a new column.
However unlike other DataFramesMeta.jl macros, multi-column assignments via
`AsTable` are disallowed. The following will fail.

```
@transform df @astable begin
    $AsTable = :x
end
```

References to existing columns also follow the same
rules as other DataFramesMeta.jl macros.

### Examples

```
julia> df = DataFrame(a = [1, 2, 3], b = [4, 5, 6]);

julia> d = @rtransform df @astable begin
           :x = 1
           y = 5
           :z = :x + y
       end
3×4 DataFrame
 Row │ a      b      x      z
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      1      6
   2 │     2      5      1      6
   3 │     3      6      1      6

julia> df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 70, 80]);

julia> @by df :a @astable begin
            ex = extrema(:b)
            :min_b = first(ex)
            :max_b = last(ex)
       end
2×3 DataFrame
 Row │ a      min_b  max_b
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      6
   2 │     2     70     80

julia> new_col = "New Column";

julia> @rtransform df @astable begin
           f_a = first(:a)
           $(DOLLAR)new_col = :a + :b + f_a
           :y = :a * :b
       end
4×4 DataFrame
 Row │ a      b      New Column  y
     │ Int64  Int64  Int64       Int64
─────┼─────────────────────────────────
   1 │     1      5           7      5
   2 │     1      6           8      6
   3 │     2     70          74    140
   4 │     2     80          84    160
```

"""
macro astable(args...)
    throw(ArgumentError("@astable only works inside DataFramesMeta macros."))
end

"""
    @kwarg(args...)

Inside of DataFramesMeta.jl macros, pass keyword arguments to the underlying
DataFrames.jl function when arguments are written in "block" format.

```
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

!!! note
    This only has meaning inside DataFramesMeta.jl macros. It does not work outside
    of DataFrames.jl macros.

"""
macro kwarg(args...)
    throw(ArgumentError("@kwarg only works inside DataFramesMeta macros."))
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
    "is currently allowed with $DOLLAR"))

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
If an expression is wrapped in  `$DOLLAR(expr)`, the column is referenced by the
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
3-element Vector{Int64}:
 3
 2
 3

julia> @with(df, :x + x)
3-element Vector{Int64}:
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
2-element Vector{Int64}:
 1
 2

julia> colref = :x;

julia> @with(df, :y + $(DOLLAR)colref) # Equivalent to df[!, :y] + df[!, colref]
3-element Vector{Int64}:
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

!!! note
    Using `AsTable` inside `@with` block is currently not supported.
"""
macro with(d, body)
    esc(with_helper(d, body))
end

ASTABLE_RHS_ORDERBY_DOCS = """
In operations, it is also allowed to use `AsTable(cols)` to work with
multiple columns at once, where the columns are grouped together in a
`NamedTuple`. When `AsTable(cols)` appears in a operation, no
other columns may be referenced in the block.

Using `AsTable` in this way is useful for working with many columns
at once programmatically. For example, to order rows by the
sum of the columns `:a`, `:b`, and `:c`, write

```
@byrow sum(AsTable([:a, :b, :c]))
```

This constructs the pair

```
AsTable([:a, :b, :c]) => ByRow(sum)
```

`AsTable` on the right-hand side also allows the use of the special
column selectors `Not`, `Between`, and regular expressions. For example,
to order all rows by the product of all columns starting with `"a"`, write

```
@byrow prod(AsTable(r"^a"))
```
"""

ASTABLE_RHS_SUBSET_DOCS = """
In operations, it is also allowed to use `AsTable(cols)` to work with
multiple columns at once, where the columns are grouped together in a
`NamedTuple`. When `AsTable(cols)` appears in a operation, no
other columns may be referenced in the block.

Using `AsTable` in this way is useful for working with many columns
at once programmatically. For example, to select rows where the
sum of the columns `:a`, `:b`, and `:c` is greater than `5`, write

```
@byrow sum(AsTable([:a, :b, :c])) > 5
```

This constructs the pair

```
AsTable([:a, :b, :c]) => ByRow(t -> sum(t) > 5)
```

`AsTable` on the right-hand side also allows the use of the special
column selectors `Not`, `Between`, and regular expressions. For example,
to subset all rows where the product of all columns starting with `"a"`,
is greater than `5`, write

```
@byrow prod(AsTable(r"^a")) > 5
```
"""

ASTABLE_RHS_SELECT_TRANSFORM_DOCS = """
In operations, it is also allowed to use `AsTable(cols)` to work with
multiple columns at once, where the columns are grouped together in a
`NamedTuple`. When `AsTable(cols)` appears in a operation, no
other columns may be referenced in the block.

Using `AsTable` in this way is useful for working with many columns
at once programmatically. For example, to compute the row-wise sum of the
columns `[:a, :b, :c, :d]`, write

```
@byrow :c = sum(AsTable([:a, :b, :c, :d]))
```

This constructs the pairs

```
AsTable(nms) => ByRow(sum) => :c
```

`AsTable` on the right-hand side also allows the use of the special
column selectors `Not`, `Between`, and regular expressions. For example,
to calculate the product of all the columns beginning with the letter `"a"`,
write

```
@byrow :d = prod(AsTable(r"^a"))
```
"""
##############################################################################
##
## @subset and subset! - select row subsets
##
##############################################################################

function subset_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)

    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset($x, $(t...); (skipmissing = true,)..., $(kw...))
    end
end

function where_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)
    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset($x, $(t...); skipmissing=true, $(kw...))
    end
end

"""
    @subset(d, i...; kwargs...)

Select row subsets in `AbstractDataFrame`s and `GroupedDataFrame`s.

### Arguments

* `d` : an AbstractDataFrame or GroupedDataFrame
* `i...` : expression for selecting rows
* `kwargs...` : keyword arguments passed to `DataFrames.subset`

### Details

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
times, `@orderby` also allows `@byrow` to be placed at the beginning of a block of
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

$ASTABLE_RHS_SUBSET_DOCS

`@subset` accepts the same keyword arguments as `DataFrames.subset` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@subset(df, :a; skipmissing = false, view = true)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `subset` function.

```
@subset df begin
    :a .== 1
    @kwarg skipmissing = false
    @kwarg view = true
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

julia> df = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1,
                                    2, 1, 1, 2, 2, 2, 3, 1, 1, 2]);

julia> g = groupby(df, :x);

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

julia> df = DataFrame(a = [1, 2, missing], b = ["x", "y", missing]);

julia> @subset(df, :a .== 1)
1×2 DataFrame
 Row │ a       b
     │ Int64?  String?
─────┼─────────────────
   1 │      1  x

julia> @subset(df, :a .< 3; view = true)
2×2 SubDataFrame
 Row │ a       b
     │ Int64?  String?
─────┼─────────────────
   1 │      1  x
   2 │      2  y

julia> @subset df begin
           :a .< 3
           @kwarg view = true
       end
2×2 SubDataFrame
 Row │ a       b
     │ Int64?  String?
─────┼─────────────────
   1 │      1  x
   2 │      2  y
```
"""
macro subset(x, args...)
    esc(subset_helper(x, args...))
end

function rsubset_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)

    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset($x, $(t...); (skipmissing = true,)..., $(kw...))
    end
end


"""
    @rsubset(d, i...; kwargs...)

Row-wise version of `@subset`, i.e. all operations use `@byrow` by
default. See [`@subset`](@ref) for details.

Use this function as an alternative to placing the `.` to broadcast row-wise operations.

### Examples
```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(A=1:5, B=["apple", "pear", "apple", "orange", "pear"])
5×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     1  apple
   2 │     2  pear
   3 │     3  apple
   4 │     4  orange
   5 │     5  pear

julia> @rsubset df :A > 3
2×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     4  orange
   2 │     5  pear

julia> @rsubset df :A > 3 || :B == "pear"
3×2 DataFrame
  Row │ A      B
      │ Int64  String
 ─────┼───────────────
    1 │     2  pear
    2 │     4  orange
    3 │     5  pear
```
"""
macro rsubset(x, args...)
    esc(rsubset_helper(x, args...))
end


"""
    @where(x, args...)

Deprecated version of `@subset`, see `?@subset` for details.
"""
macro where(x, args...)
    @warn "`@where is deprecated, use `@subset` instead."
    esc(where_helper(x, args...))
end

function subset!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)

    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset!($x, $(t...); (;skipmissing = true,)..., $(kw...))
    end
end

function rsubset!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)

    t = (fun_to_vec(ex; no_dest=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $subset!($x, $(t...); (skipmissing = true,)..., $(kw...))
    end
end


"""
    @subset!(d, i...; kwargs...)

Select row subsets in `AbstractDataFrame`s and `GroupedDataFrame`s,
mutating the underlying data-frame in-place.

### Arguments

* `d` : an AbstractDataFrame or GroupedDataFrame
* `i...` : expression for selecting rows
* `kwargs` : keyword arguments passed to `DataFrames.subset!`

### Details

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

$ASTABLE_RHS_SUBSET_DOCS

`@subset!` accepts the same keyword arguments as `DataFrames.subset!` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@subset!(df, :a; skipmissing = false)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `subset!` function.

```
@subset! df begin
    :a .== 1
    @kwarg skipmissing = false
end
```

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> df = DataFrame(x = 1:3, y = [2, 1, 2]);

julia> globalvar = [2, 1, 0];

julia> @subset!(copy(df), :x .> 1)
2×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     3      2

julia> @subset!(copy(df), :x .> globalvar)
2×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     3      2

julia> @subset! copy(df) begin
           :x .> globalvar
           :y .== 3
       end
0×2 DataFrame

julia> df = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1,
                                    2, 1, 1, 2, 2, 2, 3, 1, 1, 2]);

julia> g = groupby(copy(df), :x);

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

julia> g = groupby(copy(df), :x);

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
 Row │ a       b
     │ Int64?  String?
─────┼─────────────────
   1 │      1  x
```
"""
macro subset!(x, args...)
    esc(subset!_helper(x, args...))
end


"""
    @rsubset!(d, i...)

Row-wise version of `@subset!`, i.e. all operations use `@byrow` by
default. See [`@subset!`](@ref) for details.
"""
macro rsubset!(x, args...)
    esc(rsubset!_helper(x, args...))
end


##############################################################################
##
## @orderby
##
##############################################################################
function orderby_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)
    t = (fun_to_vec(ex; gensym_names = true, outer_flags = outer_flags) for ex in exprs)
    quote
        $orderby($x, $(t...); $(kw...))
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

### Arguments

* `d`: a `DataFrame` or `GroupedDataFrame`
* `i...`: arguments on which to sort the object

### Details

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

$ASTABLE_RHS_ORDERBY_DOCS

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> d = DataFrame(x = [3, 3, 3, 2, 1, 1, 1, 2, 1, 1], n = 1:10,
                     c = ["a", "c", "b", "e", "d", "g", "f", "i", "j", "h"]);

julia> @orderby(d, -:n)
10×3 DataFrame
 Row │ x      n      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     1     10  h
   2 │     1      9  j
   3 │     2      8  i
   4 │     1      7  f
   5 │     1      6  g
   6 │     1      5  d
   7 │     2      4  e
   8 │     3      3  b
   9 │     3      2  c
  10 │     3      1  a

julia> @orderby(d, invperm(sortperm(:c, rev = true)))
10×3 DataFrame
 Row │ x      n      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     1      9  j
   2 │     2      8  i
   3 │     1     10  h
   4 │     1      6  g
   5 │     1      7  f
   6 │     2      4  e
   7 │     1      5  d
   8 │     3      2  c
   9 │     3      3  b
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

function rorderby_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)
    t = (fun_to_vec(ex; gensym_names=true, outer_flags=outer_flags) for ex in exprs)
    quote
        $orderby($x, $(t...); $(kw...))
    end
end

"""
    @rorderby(d, args...)

Row-wise version of `@orderby`, i.e. all operations use `@byrow` by
default. See [`@orderby`](@ref) for details.

Use this function as an alternative to placing the `.` to broadcast row-wise operations.

### Examples
```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(x = [8,8,-8,7,7,-7], y = [-1, 1, -2, 2, -3, 3])
6×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     8     -1
   2 │     8      1
   3 │    -8     -2
   4 │     7      2
   5 │     7     -3
   6 │    -7      3

julia> @rorderby df abs(:x) (:x * :y^3)
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     7     -3
   2 │    -7      3
   3 │     7      2
   4 │     8     -1
   5 │     8      1
   6 │    -8     -2

julia>  @rorderby df :y == 2 ? -:x : :y
6×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     7      2
   2 │     7     -3
   3 │    -8     -2
   4 │     8     -1
   5 │     8      1
   6 │    -7      3
```
"""
macro rorderby(d, args...)
    esc(rorderby_helper(d, args...))
end


##############################################################################
##
## transform & @transform
##
##############################################################################
function transform_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)
    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    :($transform($x, $(t...);  $(kw...)))
end

"""
    @transform(d, i...; kwargs...)

Add additional columns or keys based on keyword-like arguments.

### Arguments

* `d`: an `AbstractDataFrame`, or `GroupedDataFrame`
* `i...`: transformations defining new columns or keys, of the form `:y = f(:x)`
* `kwargs...`: keyword arguments passed to `DataFrames.transform`

### Returns

* `::AbstractDataFrame` or `::GroupedDataFrame`

### Details

Inputs to `@transform` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, (`:y = f(:x)`), or as a series of
keyword-like arguments. For example, the following are
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

$ASTABLE_MACRO_FLAG_DOCS

$ASTABLE_RHS_SELECT_TRANSFORM_DOCS

`@transform` accepts the same keyword arguments as `DataFrames.transform!` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@transform(gd, :x = :a .- 1; ungroup = false)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `transform!` function.

```
@transform gd begin
    :x = :a .- 1
    @kwarg ungroup = false
end
```

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

omit_nested_when(ex, when = Ref(false)) = ex, when
function omit_nested_when(ex::Expr, when = Ref(false))
    if ex.head == :macrocall && ex.args[1] in keys(DEFAULT_FLAGS)
        macroname = ex.args[1]
        if macroname == WHEN_SYM
            when[] = true
            return omit_nested_when(MacroTools.unblock(ex.args[3]), when)
        else
            new_expr, when = omit_nested_when(MacroTools.unblock(ex.args[3]), when)
            ex.args[3] = new_expr
        end
    end
    return ex, when
end

function get_when_statements(exprs)
    new_exprs = []
    when_statements = []
    seen_non_when = false
    for expr in exprs
        e, when = omit_nested_when(expr)
        if when[]
            if seen_non_when
                throw(ArgumentError("All @when statements must come first"))
            end
            push!(when_statements, e)
        else
            seen_non_when = true
            push!(new_exprs, expr)
        end
    end

    new_exprs, when_statements
end

function rtransform_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)

    exprs, whens = get_when_statements(exprs)
    if !isempty(whens)
        w = (fun_to_vec(ex; no_dest = true, gensym_names=false, outer_flags=outer_flags) for ex in whens)
        t = (fun_to_vec(ex; gensym_names=false, outer_flags=outer_flags) for ex in exprs)
        z = gensym()
        quote
            $z = $subset($copy($x), $(w...); view = true)
            $parent($transform!($z, $(t...); $(kw...)))
        end
    else
        t = (fun_to_vec(ex; gensym_names=false, outer_flags=outer_flags) for ex in exprs)
        quote
            $transform($x, $(t...); $(kw...))
        end
    end
end

"""
    @rtransform(x, args...; kwargs...)

Row-wise version of `@transform`, i.e. all operations use `@byrow` by default.
See [`@transform`](@ref) for details.

### Examples
```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(x = 1:5, y = 11:15)
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14
   5 │     5     15

julia> @rtransform(df, :a = :x + :y ^ 2, :c = :y == 13 ? 999 : 1 - :y)
5×4 DataFrame
 Row │ x      y      a      c
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1     11    122    -10
   2 │     2     12    146    -11
   3 │     3     13    172    999
   4 │     4     14    200    -13
   5 │     5     15    230    -14
```
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
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)

    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $transform!($x, $(t...); $(kw...))
    end
end

"""
    @transform!(d, i...; kwargs...)

Mutate `d` inplace to add additional columns or keys based on keyword-like
arguments and return it. No copies of existing columns are made.

### Arguments

* `d` : an `AbstractDataFrame`, or `GroupedDataFrame`
* `i...` : transformations of the form `:y = f(:x)` defining new columns or keys
* `kwargs...`: keyword arguments passed to `DataFrames.transform!`

### Returns

* `::DataFrame` or a `GroupedDataFrame`

### Details

Inputs to `@transform!` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, (`:y = f(:x)`), or as a series of
keyword-like arguments. For example, the following are
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

$ASTABLE_MACRO_FLAG_DOCS

$ASTABLE_RHS_SELECT_TRANSFORM_DOCS

`@transform!` accepts the same keyword arguments as `DataFrames.transform!` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@transform!(gd, :x = :a .- 1; ungroup = false)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `transform!` function.

```
@transform! gd begin
    :x = :a .- 1
    @kwarg ungroup = false
end
```

### Examples

```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(A = 1:3, B = [2, 1, 2]);

julia> df2 = @transform!(df, :a = 2 * :A, :x = :A .+ :B)
3×4 DataFrame
 Row │ A      B      a      x
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      2      3
   2 │     2      1      4      3
   3 │     3      2      6      5

julia> df === df2
true
```
"""
macro transform!(x, args...)
    esc(transform!_helper(x, args...))
end

function rtransform!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)

    t = (fun_to_vec(ex; gensym_names=false, outer_flags=outer_flags) for ex in exprs)
    quote
        $transform!($x, $(t...); $(kw...))
    end
end

"""
    @rtransform!(x, args...; kwargs...)

Row-wise version of `@transform!`, i.e. all operations use `@byrow` by
default. See [`@transform!`](@ref) for details."""
macro rtransform!(x, args...)
    esc(rtransform!_helper(x, args...))
end

##############################################################################
##
## @select - select and transform columns
##
##############################################################################

function select_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)

    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $select($x, $(t...); $(kw...))
    end
end

"""
    @select(d, i...; kwargs...)

Select and transform columns.

### Arguments

* `d` : an `AbstractDataFrame` or `GroupedDataFrame`
* `i` :  transformations of the form `:y = f(:x)` specifying
new columns in terms of existing columns or symbols to specify existing columns
* `kwargs` : keyword arguments passed to `DataFrames.select`

### Returns

* `::AbstractDataFrame` or a `GroupedDataFrame`

### Details

Inputs to `@select` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation or selector, or as a series of
arguments and keyword-like arguments arguments. For example, the following are
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
beginning of a block of selections (i.e. `@byrow begin... end`).
All transformations in the block will operate by row.

$ASTABLE_MACRO_FLAG_DOCS

$ASTABLE_RHS_SELECT_TRANSFORM_DOCS

`@select` accepts the same keyword arguments as `DataFrames.select` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@select(df, :a; copycols = false)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `select` function.

```
@select gd begin
    :a
    @select copycols = false
end
```

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

function rselect_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)

    t = (fun_to_vec(ex; gensym_names=false, outer_flags=outer_flags) for ex in exprs)
    quote
        $select($x, $(t...); $(kw...))
    end
end

"""
    @rselect(x, args...; kwargs...)

Row-wise version of `@select`, i.e. all operations use `@byrow` by
default. See [`@select`](@ref) for details.

### Examples
```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(x = 1:5, y = 10:14)
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     10
   2 │     2     11
   3 │     3     12
   4 │     4     13
   5 │     5     14

julia> @rselect(df, :x, :A = mod(:y, :x) == 0 ? 99 : :x)
5×2 DataFrame
 Row │ x      A
     │ Int64  Int64
─────┼──────────────
   1 │     1     99
   2 │     2      2
   3 │     3     99
   4 │     4      4
   5 │     5      5
```
"""
macro rselect(x, args...)
    esc(rselect_helper(x, args...))
end


##############################################################################
##
## @select! - in-place select and transform columns
##
##############################################################################

function select!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)

    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)
    quote
        $select!($x, $(t...); $(kw...))
    end
end

"""
    @select!(d, i...; kwargs...)

Mutate `d` in-place to retain only columns or transformations specified by `e` and return it. No copies of existing columns are made.

### Arguments

* `d` : an AbstractDataFrame
* `i` : transformations of the form `:y = f(:x)` specifying
new columns in terms of existing columns or symbols to specify existing columns
* `kwargs` : keyword arguments passed to `DataFrames.select!`

### Returns

* `::DataFrame`

### Details

Inputs to `@select!` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation or selector, or as a series of
arguments and keyword-like arguments. For example, the following are
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

$ASTABLE_MACRO_FLAG_DOCS

$ASTABLE_RHS_SELECT_TRANSFORM_DOCS

`@select!` accepts the same keyword arguments as `DataFrames.select!` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@select!(gd, :a; ungroup = false)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `select!` function.

```
@select! gd begin
    :a
    @kwarg ungroup = false
end
```

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

function rselect!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)

    t = (fun_to_vec(ex; gensym_names=false, outer_flags=outer_flags) for ex in exprs)
    quote
        $select!($x, $(t...); $(kw...))
    end
end

"""
    @rselect!(x, args...; kwargs...)

Row-wise version of `@select!`, i.e. all operations use `@byrow` by
default. See [`@select!`](@ref) for details.
"""
macro rselect!(x, args...)
    esc(rselect!_helper(x, args...))
end

##############################################################################
##
## @combine - summarize a grouping operation
##
##############################################################################

function combine_helper(x, args...; deprecation_warning = false)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)

    deprecation_warning && @warn "`@based_on` is deprecated. Use `@combine` instead."

    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)

    quote
        $combine($x, $(t...); $(kw...))
    end
end

"""
    @combine(x, args...; kwargs...)

Summarize a grouping operation

### Arguments

* `x` : a `GroupedDataFrame` or `AbstractDataFrame`
* `args...` : transformations defining new columns, of the form `:y = f(:x)`
* `kwargs`: : keyword arguments passed to `DataFrames.combine`

### Results

* A `DataFrame` or a `GroupedDataFrame`

### Details

Inputs to `@combine` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, or as a series of keyword-like arguments.
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

$ASTABLE_MACRO_FLAG_DOCS

`@combine` accepts the same keyword arguments as `DataFrames.combine` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@combine(gd, :x = first(:a); ungroup = false)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `combine` function.

```
@combine gd begin
    :x = first(:a)
    @kwarg ungroup = false
end
```

### Examples

```julia
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
    # Handle keyword arguments initially due the grouping instruction, what
    if x isa Expr && x.head === :parameters
        # with keyword arguments, everything is shifted to
        # the right
        new_what = args[1]
        args = (what, args[2:end]...)
        what = new_what
    end

    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)

    t = (fun_to_vec(ex; gensym_names = false, outer_flags = outer_flags) for ex in exprs)

    quote
        $combine($groupby($x, $what), $(t...); $(kw...))
    end
end

"""
    @by(d::AbstractDataFrame, cols, e...; kwargs...)

Split-apply-combine in one step.

### Arguments

* `d` : an AbstractDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `e` :  keyword-like arguments, of the form `:y = f(:x)` specifying
new columns in terms of column groupings
* `kwargs` : keyword arguments passed to `DataFrames.combine`

### Returns

* `::DataFrame` or a `GroupedDataFrame`

### Details

Transformation inputs to `@by` can come in two formats: a `begin ... end` block,
in which case each line in the block is a separate
transformation, or as a series of keyword-like arguments.
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

$ASTABLE_MACRO_FLAG_DOCS

`@by` accepts the same keyword arguments as `DataFrames.combine` and can be added in
two ways. When inputs are given as multiple arguments, they are added at the end after
a semi-colon `;`, as in

```
@by(ds, :g, :x = first(:a); ungroup = false)
```

When inputs are given in "block" format, the last lines may be written
`@kwarg key = value`, which indicates keyword arguments to be passed to `combine` function.

```
@by df :a begin
    :x = first(:a)
    @kwarg ungroup = false
end
```

Though `@by` performs both `groupby` and `combine`, `@by` only forwards keyword arguments
to `combine`, and not `groupby`. To pass keyword arguments to `groupby`, perform the
`groupby` and `@combine` steps separately.

### Examples

```julia
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

##############################################################################
##
## @distinct - distinct row selection
##
##############################################################################

function make_distinct(x::AbstractDataFrame, @nospecialize(t...))    
    if isempty(t)
        DataFrames.unique(x)
    else        
        tmp = DataFrames.select(x, t...; copycols = false)
        rowidxs = (!).(DataFrames.nonunique(tmp))
        (x)[rowidxs, :]                
    end
end

function make_distinct(x::GroupedDataFrame, @nospecialize(t...))
    throw(ArgumentError("@distinct with a GroupedDataFrame is reserved"))
end

function distinct_helper(x, args...)    
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)    
    t = (fun_to_vec(ex; no_dest = true, outer_flags=outer_flags) for ex in exprs)
    quote            
        $DataFramesMeta.make_distinct($x, $(t...); $(kw...))
    end
end

"""
    @distinct(d, args...)

Return the first occurrence of unique rows in an `AbstractDataFrame` according 
to given combinations of values in selected columns or their transformation. 
`args` can be most column selectors or transformation accepted by `select`. 
Users should note that `@distinct` differs from `unique` in DataFrames.jl,
such that `@distinct(df, :x,:y)` is not the same as `unique(df, [:x,:y])`. 
See  **Details** for a discussion of these differences.


### Arguments

* `d` : an AbstractDataFrame
* `args...` :  transformations of the form `:x` designating
symbols to specify columns or `f(:x)` specifying their transformations 

### Returns

* `::AbstractDataFrame`

Inputs to `@distinct` can come in two formats: a `begin ... end` block, or as a series of
arguments and keyword-like arguments. For example, the following are equivalent:

```julia
@distinct df begin 
    :x + :y
end
```

and 

```
@distinct(df, :x + :y)
```

`@distinct` uses the syntax `@byrow` to wrap transformations in the `ByRow` 
function wrapper from DataFrames, apply a function row-wise, similar to 
broadcasting. `@distinct` allows `@byrow` at the beginning of a block of 
selections (i.e. `@byrow begin... end`). The transformation in the block 
will operate by row. For example, the following two statements are equivalent.


```
@distinct df @byrow begin 
    :x + :y
    :z + :t
end
```

and

```
@distinct df begin 
    @byrow :x + :y
    @byrow :z + :t
end
```


### Details

The implementation of `@distinct` differs from the `unique` function in DataFrames.jl. 
When `args` are present, `@distinct` relies upon an internal `select` call which produces 
an intermediate data frame containing columns of `df` specified by `args`. The unique rows
of `df` are thus determined by this intermediate data frame. This focus on `select` allows 
for multiple arguments to be passed conveniently in the form of column names or transformations.

Users should be cautious when passing function arguments as vectors. E.g., `@distinct(df, $DOLLAR[:x,:y])`
should be used instead of `@distinct(df, [:x,:y])` to avoid unexpected behaviors.

### Examples

```jldoctest
julia> using DataFramesMeta;

julia> df = DataFrame(x = 1:10, y = 10:-1:1);

julia> @distinct(df, :x .+ :y)
1×2 DataFrame
 Row │ x      y      
     │ Int64  Int64  
─────┼───────────────
   1 │     1      10   

julia> @distinct df begin
            :x .+ :y
        end
1×2 DataFrame
 Row │ x      y      
     │ Int64  Int64  
─────┼───────────────
   1 │     1      10   
```
"""
macro distinct(d, args...)
    esc(distinct_helper(d, args...))
end

function rdistinct_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)    
    t = (fun_to_vec(ex; no_dest = true, outer_flags=outer_flags) for ex in exprs)
    quote            
        $DataFramesMeta.make_distinct($x, $(t...); $(kw...))
    end
end

"""
    rdistinct(d, args...)

Row-wise version of `@distinct`, i.e. all operations use `@byrow` by
default. See [`@distinct`](@ref) for details.

### Examples
```julia
julia> using DataFramesMeta

julia> df = DataFrame(x = 1:5, y = 5:-1:1)
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     5
   2 │     2     4
   3 │     3     3
   4 │     4     2
   5 │     5     1
   
julia> @rdistinct(df, :x + :y)
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     5
```
"""
macro rdistinct(d, args...)
    esc(rdistinct_helper(d, args...))
end

##############################################################################
##
## @distinct! - in-place distinct row selection
##
##############################################################################

function make_distinct!(x::AbstractDataFrame, @nospecialize(t...))
    if isempty(t)
        DataFrames.unique!(x)
    else
        tmp = DataFrames.select(x, t...; copycols = false)
        DataFrames.deleteat!(x, DataFrames._findall(DataFrames.nonunique(tmp)))                
    end
end

function make_distinct!(x::GroupedDataFrame, @nospecialize(t...))
    throw(ArgumentError("@distinct! with a GroupedDataFrame is reserved"))
end

function distinct!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)    
    t = (fun_to_vec(ex; no_dest = true, outer_flags=outer_flags) for ex in exprs)
    quote            
        $DataFramesMeta.make_distinct!($x, $(t...); $(kw...))
    end
end

"""
    @distinct!(d, args...)

In-place selection of unique rows in an `AbstractDataFrame`.
Users should note that `@distinct!` differs from `unique!` in DataFrames.jl,
such that `@distinct!(df, [:x,:y])` is not equal to `unique(df, [:x,:y])`. 
See  **Details** for a discussion of these differences.

### Arguments

* `d` : an AbstractDataFrame
* `args...` :   transformations of the form `:x` designating
symbols to specify columns or `f(:x)` specifying their transformations 


### Returns

* `::AbstractDataFrame`

Inputs to `@distinct!` can come in two formats: a `begin ... end` block, or as a series of
arguments and keyword-like arguments. For example, the following are
equivalent:

```julia
@distinct! df begin 
    :x .+ :y
end
```

and 

```
@distinct!(df, :x .+ :y)
```

`@distinct!` uses the syntax `@byrow` to wrap transformations in
the `ByRow` function wrapper from DataFrames, apply a function row-wise,
similar to broadcasting. `@distinct!` allows `@byrow` at the beginning of a block of 
selections (i.e. `@byrow begin... end`). The transformation in the block 
will operate by row. For example, the following two statements are equivalent.


```
@distinct! df @byrow begin 
    :x + :y
    :z + :t
end
```

and

```
@distinct! df begin 
    @byrow :x + :y
    @byrow :z + :t
end

```

### Details

The implementation of `@distinct!` differs from the `unique` function in DataFrames.jl. 
When `args` are present, `@distinct!` relies upon an internal `select` call which produces 
an intermediate data frame containing columns of `df` specified by `args`. The unique rows
of `df` are thus determined by this intermediate data frame. This focus on `select` allows 
for multiple arguments to be conveniently passed in the form of column names or transformations.

Users should be cautious when passing function arguments as vectors. E.g., `@distinct(df, $DOLLAR[:x,:y])`
should be used instead of `@distinct(df, [:x,:y])` to avoid unexpected behaviors.

### Examples

```julia
julia> using DataFramesMeta;

julia> df = DataFrame(x = 1:10, y = 10:-1:1);

julia> @distinct!(df, :x .+ :y)
1×2 DataFrame
 Row │ x      y      
     │ Int64  Int64  
─────┼───────────────
   1 │     1      10   

julia> @distinct! df begin
            :x .+ :y
        end
1×2 DataFrame
 Row │ x      y      
     │ Int64  Int64  
─────┼───────────────
   1 │     1      10   
```
"""
macro distinct!(d, args...)
    esc(distinct!_helper(d, args...))
end

##############################################################################
##
## @rdistinct - select distinct rows with @byrow 
##
##############################################################################

function rdistinct!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = true)    
    t = (fun_to_vec(ex; no_dest = true, outer_flags=outer_flags) for ex in exprs)
    quote            
        $DataFramesMeta.make_distinct!($x, $(t...); $(kw...))
    end
end

"""
    rdistinct!(d, args...)

Row-wise version of `@distinct!`, i.e. all operations use `@byrow` by
default. See [`@distinct!`](@ref) for details.

### Examples
```julia
julia> using DataFramesMeta

julia> df = DataFrame(x = 1:5, y = 5:-1:1)
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     5
   2 │     2     4
   3 │     3     3
   4 │     4     2
   5 │     5     1
   
julia> @rdistinct!(df, :x + :y)
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     5
```
"""
macro rdistinct!(d, args...)
    esc(rdistinct!_helper(d, args...))
end

##############################################################################
##
## @rename - rename columns with keyword args
##
##############################################################################
function rename_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)
    t = (rename_kw_to_pair(ex) for ex in exprs)
    quote
        $DataFrames.rename($x, $pairs_to_str_pairs($(t...))...)
    end
end

"""
    @rename(d, args...)

Change column names.

### Arguments

* `d` : an AbstractDataFrame
* `args...` : expressions of the form `:new = :old` specifying the change of a column's name
from "old" to "new". The left- and right-hand side of each expression can be passed as
symbol arguments, as in `:old_col`, or strings escaped with `$DOLLAR` as in `$DOLLAR"new_col"`.
See  **Details** for a description of accepted values.

### Returns

* `::AbstractDataFrame`

Inputs to `@rename` can come in two formats: a `begin ... end` block, or as a series of
keyword-like arguments. For example, the following are equivalent:

```julia
@rename df begin
    :new_col = :old_col
end
```

and

```
@rename df :new_col = :old_col
@rename(df, :new_col = :old_col)
```

### Details

Both the left- and right-hand side of an expression specifying a column name assignment
can be either a `Symbol` or an `AbstractString` (which may contain spaces) escaped with `
$DOLLAR`. For example `:new = ...`, and `$(DOLLAR)"new" = ...` are both valid ways
of assigning a new column name.

This idea can be extended to pass arbitrary right-hand side expressions. For example,
the following are equivalent:

```
@rename(df, :new = :old1)
```

and

```
@rename(df, :new = $("old_col" * "1"))
```

The right-hand side can additionally be an `Integer`, escaped with $(DOLLAR), to indicate
column position. For example, to rename the 4th column in a data frame to a new name, write
`@rename df :newname = $(DOLLAR)`.

### Examples
```
julia> df = DataFrame(old_col1 = 1:5, old_col2 = 11:15, old_col3 = 21:25);

julia> @rename(df, :new1 = :old_col1)
5×3 DataFrame
 Row │ new1   old_col2  old_col3
     │ Int64  Int64     Int64
─────┼───────────────────────────
   1 │     1        11        21
   2 │     2        12        22
   3 │     3        13        23
   4 │     4        14        24
   5 │     5        15        25

julia> @rename(df, :new1 = :old_col1, :new2 = $(DOLLAR)"old_col2")
5×3 DataFrame
 Row │ new1   new2   old_col3
     │ Int64  Int64  Int64
─────┼────────────────────────
   1 │     1     11        21
   2 │     2     12        22
   3 │     3     13        23
   4 │     4     14        24
   5 │     5     15        25

julia> @rename(df, :new1 = $(DOLLAR)("old_col" * "1"), :new2 = :old_col2)
5×3 DataFrame
 Row │ new1   new2   old_col3
     │ Int64  Int64  Int64
─────┼────────────────────────
   1 │     1     11        21
   2 │     2     12        22
   3 │     3     13        23
   4 │     4     14        24
   5 │     5     15        25

julia> @rename df $(DOLLAR)("New with spaces") = :old_col1
5×3 DataFrame
 Row │ New with spaces  old_col2  old_col3
     │ Int64            Int64     Int64
─────┼─────────────────────────────────────
   1 │               1        11        21
   2 │               2        12        22
   3 │               3        13        23
   4 │               4        14        24
   5 │               5        15        25

julia> @rename df :new_col2 = $(DOLLAR)2
5×3 DataFrame
 Row │ old_col1  new_col2  old_col3
     │ Int64     Int64     Int64
─────┼──────────────────────────────
   1 │        1        11        21
   2 │        2        12        22
   3 │        3        13        23
   4 │        4        14        24
   5 │        5        15        25
```
"""
macro rename(x, args...)
    esc(rename_helper(x, args...))
end

function rename!_helper(x, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(x, args...; wrap_byrow = false)
    t = (rename_kw_to_pair(ex) for ex in exprs)
    quote
        $DataFrames.rename!($x, $pairs_to_str_pairs($(t...))...)
    end
end

"""
    @rename!(d, args...)

In-place modification of column names.

### Arguments

* `d` : an AbstractDataFrame
* `args...` : expressions of the form `:new = :old` specifying the change of a column's name
from "old" to "new". The left- and right-hand side of each expression can be passed as
symbol arguments, as in `:old_col`, or strings escaped with `$DOLLAR` as in `$DOLLAR"new_col"`.
See  **Details** for a description of accepted values.

### Returns

* `::AbstractDataFrame`

Inputs to `@rename!` can come in two formats: a `begin ... end` block, or as a series of
keyword-like arguments. For example, the following are equivalent:

```julia
@rename! df begin
    :new_col = :old_col
end
```

and

```
@rename!(df, :new_col = :old_col)
```

### Details

Both the left- and right-hand side of an expression specifying a column name assignment
can be either a `Symbol` or a `String`` escaped with `$DOLLAR` For example `:new = ...`,
and `$(DOLLAR)"new" = ...` are both valid ways of assigning a new column name.

This idea can be extended to pass arbitrary right-hand side expressions. For example,
the following are equivalent:

```
@rename!(df, :new = :old1)
```

and

```
@rename!(df, :new = $("old_col" * "1"))
```

### Examples
```
julia> df = DataFrame(old_col1 = rand(5), old_col2 = rand(5),old_col3 = rand(5));

julia> @rename!(df, :new1 = :old_col1)
5×3 DataFrame
 Row │ new1       old_col2   old_col3
     │ Float64    Float64    Float64
─────┼────────────────────────────────
   1 │ 0.0176206  0.493592   0.348072
   2 │ 0.861545   0.512254   0.85763
   3 │ 0.263082   0.0267507  0.696494
   4 │ 0.643179   0.299391   0.780125
   5 │ 0.731267   0.18905    0.767292

julia> df = DataFrame(old_col1 = rand(5), old_col2 = rand(5),old_col3 = rand(5));

julia> @rename!(df, :new1 = :old_col1, :new2 = $DOLLAR"old_col2")
5×3 DataFrame
 Row │ new1       new2       old_col3
     │ Float64    Float64    Float64
─────┼────────────────────────────────
   1 │ 0.0176206  0.493592   0.348072
   2 │ 0.861545   0.512254   0.85763
   3 │ 0.263082   0.0267507  0.696494
   4 │ 0.643179   0.299391   0.780125
   5 │ 0.731267   0.18905    0.767292

julia> df = DataFrame(old_col1 = rand(5), old_col2 = rand(5),old_col3 = rand(5));

julia> @rename!(df, :new1 = $DOLLAR("old_col" * "1"), :new2 = :old_col2)
5×3 DataFrame
 Row │ new1       new2       old_col3
     │ Float64    Float64    Float64
─────┼────────────────────────────────
   1 │ 0.0176206  0.493592   0.348072
   2 │ 0.861545   0.512254   0.85763
   3 │ 0.263082   0.0267507  0.696494
   4 │ 0.643179   0.299391   0.780125
   5 │ 0.731267   0.18905    0.767292
```
"""
macro rename!(x, args...)
    esc(rename!_helper(x, args...))
end

