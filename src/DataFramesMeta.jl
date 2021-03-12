module DataFramesMeta

using Reexport

@reexport using DataFrames

# Basics:
export @with, @where, @orderby, @transform, @by, @combine, @select, @eachrow,
       @transform!, @select!,
       @byrow, @byrow!, @based_on # deprecated


global const DATAFRAMES_GEQ_22 = isdefined(DataFrames, :pretty_table) ? true : false

include("linqmacro.jl")
include("eachrow.jl")


##############################################################################
##
## @with
##
##############################################################################

function addkey!(membernames, nam)
    if !haskey(membernames, nam)
        membernames[nam] = gensym()
    end
    membernames[nam]
end

onearg(e, f) = e.head == :call && length(e.args) == 2 && e.args[1] == f

mapexpr(f, e) = Expr(e.head, map(f, e.args)...)

replace_syms!(x, membernames) = x
replace_syms!(q::QuoteNode, membernames) =
    replace_syms!(Meta.quot(q.value), membernames)
replace_syms!(e::Expr, membernames) =
    if onearg(e, :^)
        e.args[2]
    elseif onearg(e, :_I_)
        @warn "_I_() for escaping variables is deprecated, use cols() instead"
        addkey!(membernames, :($(e.args[2])))
    elseif onearg(e, :cols)
        addkey!(membernames, :($(e.args[2])))
    elseif e.head == :quote
        addkey!(membernames, Meta.quot(e.args[1]) )
    elseif e.head == :.
        replace_dotted!(e, membernames)
    else
        mapexpr(x -> replace_syms!(x, membernames), e)
    end

"""
    @col(kw)

`@col` transforms an expression of the form `z = :x + :y` into it's equivalent in
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
julia> @col z = :x + :y
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

julia> DataFrames.transform(df, @col z = :x .* :y)
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

is_simple_function_call(x) = false
function is_simple_function_call(expr::Expr)
    (expr.head == :call
        && length(expr.args) >= 2
        && expr.args[1] isa Symbol
        && all(x -> x isa QuoteNode || onearg(x, :cols), expr.args[2:end]))
end

is_simple_broadcast_call(x) = false
function is_simple_broadcast_call(expr::Expr)
    (expr.head == :.
        && length(expr.args) == 2
        && expr.args[1] isa Symbol
        && expr.args[2] isa Expr
        && expr.args[2].head == :tuple
        && all(x -> x isa QuoteNode || onearg(x, :cols), expr.args[2:end]))
end

function args_to_selectors(v)
    t = map(v) do arg
        if arg isa QuoteNode
            arg
        elseif onearg(arg, :cols)
            @show arg.args[2]
            arg.args[2]
        else
            Throw(ArgumentError("This path should not be reached, arg: $(arg)"))
        end
    end

    Expr(:vect, t...)
end

# `nolhs` needs to be `true` when we have syntax of the form
# `@combine(gd, fun(:x, :y))` where `fun` returns a `table` object.
# We don't create the "new name" pair because new names are given
# by the table.
function fun_to_vec(kw::Expr; nolhs::Bool = false, gensym_names::Bool = false)
    # nolhs: f(:x) where f returns a Table
    # !nolhs, y = g(:x)
    if !(kw.head === :(=) || kw.head === :kw || nolhs)
        throw(ArgummentError("Expressions not of the form `y = f(:x)` currently disallowed."))
    end

    # y = :x
    if nolhs == false && length(kw.args) == 2
       x = kw.args[2]
       if x isa QuoteNode || onearg(x, :cols)

            source = Expr(:vect, x isa QuoteNode ? x : x.args[2])
            fun = identity
            dest = QuoteNode(kw.args[1])

            t = quote
                $source => $fun => $dest
            end

            return t
        end
    end

    function_expr = nolhs ? kw : kw.args[2]
    # check cases where we can avoid creating an anonymous function
    # f(:x, :y) into [:x, :y] => f => :z # nolhs == false
    if is_simple_function_call(function_expr)
        # extract source symbols from quotenodes
        source = args_to_selectors(function_expr.args[2:end])
        fun = function_expr.args[1]
        # some normal-looking calls are actually broadcasts, like
        # .+ .- etc., if we just pass on those symbols as function calls
        # we get UndefVarErrors
        # instead we transform the symbols to their non-broadcast versions
        # and then use broadcast wrappers
        if startswith(string(fun), '.')
            f_sym_without_dot = Symbol(chop(string(fun), head = 1, tail = 0))
            fun = :(DataFrames.ByRow($f_sym_without_dot))
        end

    # f.(:x, ...) into [:x, ...] => ByRow(f)
    elseif is_simple_broadcast_call(function_expr)
        # extract source symbols from quotenodes
        source = args_to_selectors(function_expr.args[2].args)
        f = function_expr.args[1]
        fun = :(DataFrames.ByRow($f))

    # everything else goes through the normal replacement pipeline
    # which results in a new anonymous function
    else
        membernames = Dict{Any, Symbol}()

        body = replace_syms!(function_expr, membernames)

        source = Expr(:vect, keys(membernames)...)
        inputargs = Expr(:tuple, values(membernames)...)

        fun = quote
            $inputargs -> begin
                $body
            end
        end
    end

    # @combine(gd, (a = :x, b = :y))
    if nolhs
        if gensym_names
            # [:x] => _f => Symbol("###343")
            dest = QuoteNode(gensym())
            t = quote
                DataFramesMeta.make_source_concrete($(source)) =>
                $fun =>
                $dest
            end
        else
            # [:x] => _f => AsTable
            if DATAFRAMES_GEQ_22
                t = quote
                    DataFramesMeta.make_source_concrete($(source)) =>
                    $fun =>
                    AsTable
                end
            # [:x] => _f
            else
                t = quote
                    DataFramesMeta.make_source_concrete($(source)) =>
                    $fun
                end
            end
        end
    # @select(df, y = f(:x))
    else
        if kw.args[1] isa Symbol
            # y = f(:x) becomes [:x] => _f => :y
            dest = QuoteNode(kw.args[1])
        elseif onearg(kw.args[1], :cols)
            # cols(n) = f(:x) becomes [:x] => _f => n
            dest = kw.args[1].args[2]
        end
        t = quote
            DataFramesMeta.make_source_concrete($(source)) =>
            $fun =>
            $dest
        end
    end
    return t
end
fun_to_vec(kw::QuoteNode; nolhs::Bool = false, gensym_names::Bool = false) = kw

function make_source_concrete(x::AbstractVector)
    if isempty(x) || isconcretetype(eltype(x))
        return x
    elseif all(t -> t isa Union{AbstractString, Symbol}, x)
        return Symbol.(x)
    else
        throw(ArgumentError("Column references must be either all the same " *
                            "type or a a combination of `Symbol`s and strings"))
    end
end

protect_replace_syms!(e, membernames) = e
function protect_replace_syms!(e::Expr, membernames)
    if e.head == :quote
        e
    else
        replace_syms!(e, membernames)
    end
end

function replace_dotted!(e, membernames)
    x_new = replace_syms!(e.args[1], membernames)
    y_new = protect_replace_syms!(e.args[2], membernames)
    Expr(:., x_new, y_new)
end

getsinglecolumn(df, s::DataFrames.ColumnIndex) = df[!, s]
getsinglecolumn(df, s) = throw(ArgumentError("Only indexing with Symbols, strings and integers " *
    "is currently allowed with cols"))

function with_helper(d, body)
    membernames = Dict{Any, Symbol}()
    funname = gensym()
    body = replace_syms!(body, membernames)
    source = Expr(:vect, keys(membernames)...)
    _d = gensym()
    quote
        $_d = $d
        function $funname($(values(membernames)...))
            $body
        end
        $funname((DataFramesMeta.getsinglecolumn($_d, s) for s in  DataFramesMeta.make_source_concrete($source))...)
    end
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
## @where - select row subsets
##
##############################################################################

function where_helper(x, args...)
    t = (fun_to_vec(arg; nolhs = true, gensym_names = true) for arg in args)
    quote
        $where($x, $(t...))
    end
end

function df_to_bool(res::AbstractDataFrame)
    if any(t -> !(t isa AbstractVector{<:Union{Missing, Bool}}), eachcol(res))
        throw(ArgumentError("All arguments in @where must return an " *
                            "AbstractVector{<:Union{Missing, Bool}}"))
    end

    return reduce((x, y) -> x .& y, eachcol(res)) .=== true
end

function where(df::AbstractDataFrame, @nospecialize(args...))
    res = DataFrames.select(df, args...; copycols = false)
    tokeep = df_to_bool(res)
    df[tokeep, :]
end

function where(gd::GroupedDataFrame, @nospecialize(args...))
    res = DataFrames.select(gd, args...; copycols = false, keepkeys = false)
    tokeep = df_to_bool(res)
    parent(gd)[tokeep, :]
end

function where(df::SubDataFrame, @nospecialize(args...))
    res = DataFrames.select(df, args...)
    tokeep = df_to_bool(res)
    df[tokeep, :]
end

"""
    @where(d, i...)

Select row subsets in `AbstractDataFrame`s and `GroupedDataFrame`s.

### Arguments

* `d` : an AbstractDataFrame or GroupedDataFrame
* `i...` : expression for selecting rows

Multiple `i` expressions are "and-ed" together.

If given a `GroupedDataFrame`, `@where` applies transformations by
group, and returns a fresh `DataFrame` containing the rows
for which the generated values are all `true`.

!!! note
    `@where` treats `missing` values as `false` when filtering rows.
    Unlike `DataFrames.filter` and other boolean operations with
    `missing`, `@where` will *not* error on missing values, and
    will only keep `true` values.

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> df = DataFrame(x = 1:3, y = [2, 1, 2]);

julia> globalvar = [2, 1, 0];

julia> @where(df, :x .> 1)
2×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 2     │ 1     │
│ 2   │ 3     │ 2     │

julia> @where(df, :x .> globalvar)
2×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 2     │ 1     │
│ 2   │ 3     │ 2     │

julia> @where(df, :x .> globalvar, :y .== 3)
0×2 DataFrame

julia> d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1,
                                    2, 1, 1, 2, 2, 2, 3, 1, 1, 2]);

julia> g = groupby(d, :x);

julia> @where(g, :n .> mean(:n))
8×2 DataFrame
│ Row │ n     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 12    │ 1     │
│ 2   │ 13    │ 1     │
│ 3   │ 15    │ 2     │
│ 4   │ 16    │ 2     │
│ 5   │ 17    │ 3     │
│ 6   │ 18    │ 1     │
│ 7   │ 19    │ 1     │
│ 8   │ 20    │ 2     │

julia> @where(g, :n .== first(:n))
3×2 DataFrame
│ Row │ n     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 5     │ 1     │
│ 3   │ 8     │ 2     │

julia> d = DataFrame(a = [1, 2, missing], b = ["x", "y", missing]);

julia> @where(d, :a .== 1)
1×2 DataFrame
│ Row │ a      │ b       │
│     │ Int64? │ String? │
├─────┼────────┼─────────┤
│ 1   │ 1      │ x       │
```
"""
macro where(x, args...)
    esc(where_helper(x, args...))
end


##############################################################################
##
## @orderby
##
##############################################################################

function orderby_helper(x, args...)
    t = (fun_to_vec(arg; nolhs = true, gensym_names = true) for arg in args)
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

### Arguments

* `d` : an AbstractDataFrame
* `i...` : expression for sorting

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> d = DataFrame(x = [3, 3, 3, 2, 1, 1, 1, 2, 1, 1], n = 1:10);

julia> @orderby(d, -1 .* :n)
10×2 DataFrame
│ Row │ x     │ n     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 10    │
│ 2   │ 1     │ 9     │
│ 3   │ 2     │ 8     │
│ 4   │ 1     │ 7     │
│ 5   │ 1     │ 6     │
│ 6   │ 1     │ 5     │
│ 7   │ 2     │ 4     │
│ 8   │ 3     │ 3     │
│ 9   │ 3     │ 2     │
│ 10  │ 3     │ 1     │

julia> @orderby(d, :x, :n .- mean(:n))
10×2 DataFrame
│ Row │ x     │ n     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 5     │
│ 2   │ 1     │ 6     │
│ 3   │ 1     │ 7     │
│ 4   │ 1     │ 9     │
│ 5   │ 1     │ 10    │
│ 6   │ 2     │ 4     │
│ 7   │ 2     │ 8     │
│ 8   │ 3     │ 1     │
│ 9   │ 3     │ 2     │
│ 10  │ 3     │ 3     │
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

    t = (fun_to_vec(arg) for arg in args)

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

### Examples

```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(A = 1:3, B = [2, 1, 2]);

julia> @transform(df, a = 2 * :A, x = :A .+ :B)
3×4 DataFrame
│ Row │ A     │ B     │ a     │ x     │
│     │ Int64 │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 2     │ 3     │
│ 2   │ 2     │ 1     │ 4     │ 3     │
│ 3   │ 3     │ 2     │ 6     │ 5     │
```
"""
macro transform(x, args...)
    esc(transform_helper(x, args...))
end

##############################################################################
##
## transform! & @transform!
##
##############################################################################


function transform!_helper(x, args...)

    t = (fun_to_vec(arg) for arg in args)

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

### Examples

```jldoctest
julia> using DataFramesMeta

julia> df = DataFrame(A = 1:3, B = [2, 1, 2]);

julia> df2 = @transform!(df, a = 2 * :A, x = :A .+ :B)
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
## @combine - summarize a grouping operation
##
##############################################################################

function combine_helper(x, args...; deprecation_warning = false)
    deprecation_warning && @warn "`@based_on` is deprecated. Use `@combine` instead."

    # Only allow one argument when returning a Table object
    if length(args) == 1 &&
        !(first(args) isa QuoteNode) &&
        !(first(args).head == :(=) || first(args).head == :kw)

        t = fun_to_vec(first(args); nolhs = true)
        # 0.22: No pair as first arg, needs AsTable in other args to return table
        if DATAFRAMES_GEQ_22
            quote
                $DataFrames.combine($x, $t)
            end
        # 0.21: Pair as first arg, other args can't return table
        else
            quote
                $DataFrames.combine($t, $x)
            end
        end
    else
        t = (fun_to_vec(arg) for arg in args)
        quote
            $DataFrames.combine($x, $(t...))
        end
    end
end

"""
    @combine(x, args...)

Summarize a grouping operation

### Arguments

* `x` : a `GroupedDataFrame` or `AbstractDataFrame`
* `args...` : keyword arguments defining new columns

### Examples

```jldoctest
julia> using DataFramesMeta

julia> d = DataFrame(
            n = 1:20,
            x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2]);

julia> g = groupby(d, :x);

julia> @combine(g, nsum = sum(:n))
3×2 DataFrame
│ Row │ x     │ nsum  │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 3     │ 27    │
│ 2   │ 1     │ 99    │
│ 3   │ 2     │ 84    │

julia> @combine(g, x2 = 2 * :x, nsum = sum(:n))
20×3 DataFrame
│ Row │ x     │ x2    │ nsum  │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 3     │ 6     │ 27    │
│ 2   │ 3     │ 6     │ 27    │
│ 3   │ 3     │ 6     │ 27    │
│ 4   │ 3     │ 6     │ 27    │
│ 5   │ 3     │ 6     │ 27    │
│ 6   │ 1     │ 2     │ 99    │
│ 7   │ 1     │ 2     │ 99    │
⋮
│ 13  │ 1     │ 2     │ 99    │
│ 14  │ 1     │ 2     │ 99    │
│ 15  │ 2     │ 4     │ 84    │
│ 16  │ 2     │ 4     │ 84    │
│ 17  │ 2     │ 4     │ 84    │
│ 18  │ 2     │ 4     │ 84    │
│ 19  │ 2     │ 4     │ 84    │
│ 20  │ 2     │ 4     │ 84    │

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
    if length(args) == 1 &&
        !(first(args) isa QuoteNode) &&
        !(first(args).head == :(=) || first(args).head == :kw)

        t = fun_to_vec(first(args); nolhs = true)
        # 0.22: No pair as first arg, needs AsTable in other args to return table
        if DATAFRAMES_GEQ_22
            quote
                $DataFrames.combine($groupby($x, $what), $t)
            end
        # 0.21: Pair as first arg, other args can't return table
        else
            quote
                $DataFrames.combine($t, $groupby($x, $what))
            end
        end
    else
        t = (fun_to_vec(arg) for arg in args)
        quote
            $DataFrames.combine($groupby($x, $what), $(t...))
        end
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

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> df = DataFrame(
            a = repeat(1:4, outer = 2),
            b = repeat(2:-1:1, outer = 4),
            c = 1:8);

julia> @by(df, :a, d = sum(:c))
4×2 DataFrame
│ Row │ a     │ d     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 8     │
│ 3   │ 3     │ 10    │
│ 4   │ 4     │ 12    │

julia> @by(df, :a, d = 2 * :c)
8×2 DataFrame
│ Row │ a     │ d     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 2     │
│ 2   │ 1     │ 10    │
│ 3   │ 2     │ 4     │
│ 4   │ 2     │ 12    │
│ 5   │ 3     │ 6     │
│ 6   │ 3     │ 14    │
│ 7   │ 4     │ 8     │
│ 8   │ 4     │ 16    │

julia> @by(df, :a, c_sum = sum(:c), c_mean = mean(:c))
4×3 DataFrame
│ Row │ a     │ c_sum │ c_mean  │
│     │ Int64 │ Int64 │ Float64 │
├─────┼───────┼───────┼─────────┤
│ 1   │ 1     │ 6     │ 3.0     │
│ 2   │ 2     │ 8     │ 4.0     │
│ 3   │ 3     │ 10    │ 5.0     │
│ 4   │ 4     │ 12    │ 6.0     │

julia> @by(df, :a, c = :c, c_mean = mean(:c))
8×3 DataFrame
│ Row │ a     │ c     │ c_mean  │
│     │ Int64 │ Int64 │ Float64 │
├─────┼───────┼───────┼─────────┤
│ 1   │ 1     │ 1     │ 3.0     │
│ 2   │ 1     │ 5     │ 3.0     │
│ 3   │ 2     │ 2     │ 4.0     │
│ 4   │ 2     │ 6     │ 4.0     │
│ 5   │ 3     │ 3     │ 5.0     │
│ 6   │ 3     │ 7     │ 5.0     │
│ 7   │ 4     │ 4     │ 6.0     │
│ 8   │ 4     │ 8     │ 6.0     │

```
"""
macro by(x, what, args...)
    esc(by_helper(x, what, args...))
end


##############################################################################
##
## @select - select and transform columns
##
##############################################################################

function select_helper(x, args...)
    t = (fun_to_vec(arg) for arg in args)

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

### Examples

```jldoctest
julia> using DataFrames, DataFramesMeta

julia> df = DataFrame(a = repeat(1:4, outer = 2), b = repeat(2:-1:1, outer = 4), c = 1:8);

julia> @select(df, :c, :a)
8×2 DataFrame
│ Row │ c     │ a     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 4     │ 4     │
│ 5   │ 5     │ 1     │
│ 6   │ 6     │ 2     │
│ 7   │ 7     │ 3     │
│ 8   │ 8     │ 4     │

julia> @select(df, :c, x = :b + :c)
8×2 DataFrame
│ Row │ c     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 2     │ 3     │
│ 3   │ 3     │ 5     │
│ 4   │ 4     │ 5     │
│ 5   │ 5     │ 7     │
│ 6   │ 6     │ 7     │
│ 7   │ 7     │ 9     │
│ 8   │ 8     │ 9     │
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
    t = (fun_to_vec(arg) for arg in args)

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

### Examples

```jldoctest
julia> using DataFrames, DataFramesMeta

julia> df = DataFrame(a = repeat(1:4, outer = 2), b = repeat(2:-1:1, outer = 4), c = 1:8);

julia> df2 = @select!(df, :c, :a)
8×2 DataFrame
│ Row │ c     │ a     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 4     │ 4     │
│ 5   │ 5     │ 1     │
│ 6   │ 6     │ 2     │
│ 7   │ 7     │ 3     │
│ 8   │ 8     │ 4     │

julia> df === df2
true



julia> df = DataFrame(a = repeat(1:4, outer = 2), b = repeat(2:-1:1, outer = 4), c = 1:8);

julia> df2 = @select!(df, :c, x = :b + :c)
8×2 DataFrame
│ Row │ c     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 2     │ 3     │
│ 3   │ 3     │ 5     │
│ 4   │ 4     │ 5     │
│ 5   │ 5     │ 7     │
│ 6   │ 6     │ 7     │
│ 7   │ 7     │ 9     │
│ 8   │ 8     │ 9     │

julia> df === df2
true
```
"""
macro select!(x, args...)
    esc(select!_helper(x, args...))
end

end # module
