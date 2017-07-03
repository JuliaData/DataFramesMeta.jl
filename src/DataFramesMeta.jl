module DataFramesMeta

using DataFrames

# Basics:
export @with, @ix, @where, @orderby, @transform, @by, @based_on, @select
export where, orderby, transform

include("compositedataframe.jl")
include("linqmacro.jl")
include("byrow.jl")


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
replace_syms!(e::Expr, membernames) =
    if onearg(e, :^)
        e.args[2]
    elseif onearg(e, :_I_)
        addkey!(membernames, :($(e.args[2])))
    elseif e.head == :quote
        addkey!(membernames, Meta.quot(e.args[1]) )
    elseif e.head == :.
        replace_dotted!(e, membernames)
    else
        mapexpr(x -> replace_syms!(x, membernames), e)
    end

protect_replace_syms!(e, membernames) = e
protect_replace_syms!(e::Expr, membernames) =
    e.head == :quote ? e : replace_syms!(e, membernames)

function replace_dotted!(e, membernames)
    x_new = replace_syms!(e.args[1], membernames)
    y_new = protect_replace_syms!(e.args[2], membernames)
    :($x_new.($y_new))
end

function with_helper(d, body)
    membernames = Dict{Any, Symbol}()
    body = replace_syms!(body, membernames)
    funargs = map(x -> :( getindex($d, $x) ), collect(keys(membernames)))
    funname = gensym()
    return quote
        function $funname($(collect(values(membernames))...))
            $body
        end
        $funname($(funargs...))
    end
end

"""
```julia
@with(d, expr)
```

`@with` allows DataFrame columns or Associative keys to be referenced as symbols.

### Arguments

* `d` : an AbstractDataFrame or Associative type
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
@with(d, :a + :b + 1)
```

becomes

```julia
tempfun(a,b) = a + b + 1
tempfun(d[:a], d[:b])
```

All of the other DataFramesMeta macros are based on `@with`.

If an expression is wrapped in `^(expr)`, `expr` gets passed through untouched.
If an expression is wrapped in  `_I_(expr)`, the column is referenced by the
variable `expr` rather than a symbol.

### Examples

```julia
y = 3
d = Dict(:s => 3, :y => 44, :d => 5)

@with(d, :s + :y + y)

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

`@with` creates a function, so scope within `@with` is a *hard scope*,
as with `do`-blocks or other function definitions. Variables in the parent
can be read. Writing to variables in the parent scope differs depending on
the type of scope of the parent. If the parent scope is a global scope, then
a variable cannot be assigned without using the `global` keyword. If the parent
scope is a local scope (inside a function or let block for example), the `global`
keyword is not needed to assign to that parent scope.

"""
macro with(d, body)
    esc(with_helper(d, body))
end


##############################################################################
##
## @ix - row and row/col selector
##
##############################################################################

ix_helper(d, arg) = quote
    let d = $d
        $d[DataFramesMeta.@with($d, $arg),:]
    end
end

ix_helper(d, arg, moreargs...) = quote
    let d = $d
        getindex(d, DataFramesMeta.@with(d, $arg), $(moreargs...))
    end
end

macro ix(d, args...)
    Base.depwarn("`@ix` is deprecated; use `@where` and `@select`.", Symbol("@ix"))
    esc(ix_helper(d, args...))
end


##############################################################################
##
## @where - select row subsets
##
##############################################################################

where(d::AbstractDataFrame, arg) = d[arg, :]
where(d::AbstractDataFrame, f::Function) = d[f(d), :]
where(g::GroupedDataFrame, f::Function) = g[Bool[f(x) for x in g]]

collect_ands(x::Expr) = x
collect_ands(x::Expr, y::Expr) = :($x & $y)
collect_ands(x::Expr, y...) = :($x & $(collect_ands(y...)))

function where_helper(d, args...)
    with_args = :($DataFramesMeta.@with(_DF, $(collect_ands(args...))))
    :( $DataFramesMeta.where($d, _DF -> $with_args) )
end

"""
```julia
@where(d, i...)
```

Select row subsets in AbstractDataFrames or groups in GroupedDataFrames.

### Arguments

* `d` : an AbstractDataFrame or GroupedDataFrame
* `i...` : expression for selecting rows

Multiple `i` expressions are "and-ed" together.

### Examples

```julia
df = DataFrame(x = 1:3, y = [2, 1, 2])
x = [2, 1, 0]

@where(df, :x .> 1)
@where(df, :x .> x)
@where(df, :x .> x, :y .== 3)

d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x)
@where(d, :x .== 3)
@where(g, length(:x) > 5))   # pick out some groups
```
"""
macro where(d, args...)
    esc(where_helper(d, args...))
end


##############################################################################
##
## select - select columns
##
##############################################################################

Base.select(d::AbstractDataFrame, arg) = d[arg]


##############################################################################
##
## @orderby
##
##############################################################################

function orderby(d::AbstractDataFrame, args...)
    D = typeof(d)(args...)
    d[sortperm(D), :]
end

orderby(d::AbstractDataFrame, f::Function) = d[sortperm(f(d)), :]
orderby(g::GroupedDataFrame, f::Function) = g[sortperm([f(x) for x in g])]

orderbyconstructor(d::AbstractDataFrame) = (x...) -> DataFrame(Any[x...])
orderbyconstructor(d) = x -> x

function orderby_helper(d, args...)
    construct_args = :(DataFramesMeta.orderbyconstructor(_D)($(args...)))
    with_args = :(DataFramesMeta.@with(_DF, $construct_args))
    quote
        let _D = $d
            DataFramesMeta.orderby(_D, _DF -> $with_args)
        end
    end
end

"""
```julia
@orderby(d, i...)
```

Sort by criteria. Normally used to sort groups in GroupedDataFrames.

### Arguments

* `d` : an AbstractDataFrame or GroupedDataFrame
* `i...` : expression for sorting

The variable `_DF` can be used in expressions to refer to the whole DataFrame.

### Examples

```julia
d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x)
orderby(g, x -> mean(x[:n]))
```

"""
macro orderby(d, args...)
    # I don't esc just the input because I want _DF to be visible to the user
    esc(orderby_helper(d, args...))
end


##############################################################################
##
## transform & @transform
##
##############################################################################

function transform(d::Union{AbstractDataFrame, Associative}; kwargs...)
    result = copy(d)
    for (k, v) in kwargs
        result[k] = isa(v, Function) ? v(d) : v
    end
    return result
end

function transform(g::GroupedDataFrame; kwargs...)
    result = DataFrame(g)
    idx2 = cumsum(Int[size(g[i],1) for i in 1:length(g)])
    idx1 = [1; 1 + idx2[1:end-1]]
    for (k, v) in kwargs
        first = v(g[1])
        result[k] = Array(eltype(first), size(result, 1))
        result[idx1[1]:idx2[1], k] = first
        for i in 2:length(g)
            result[idx1[i]:idx2[i], k] = v(g[i])
        end
    end
    return result
end

function convert_kw!(kw)
    kw.args[2] = :( _DF -> DataFramesMeta.@with(_DF, $(kw.args[2]) ) )
    kw
end

function transform_helper(x, args...)
    # convert each kw arg value to: _DF -> @with(_DF, arg)
    newargs = [args...]
    map!(convert_kw!, newargs)
    :( transform($x, $(newargs...)) )
end

"""
```julia
@transform(d, i...)
```

Add additional columns or keys based on keyword arguments.

### Arguments

* `d` : an Associative type, AbstractDataFrame, or GroupedDataFrame
* `i...` : keyword arguments defining new columns or keys

For Associative types, `@transform` only works with keys that are symbols.

### Returns

* `::AbstractDataFrame`, `::Associative`, or `::GroupedDataFrame`

### Examples

```julia
d = Dict(:s => 3, :y => 44, :d => 5)
@transform(d, x = :y + :d)

df = DataFrame(A = 1:3, B = [2, 1, 2])
@transform(df, a = 2 * :A, x = :A + :B)
```

"""
macro transform(x, args...)
    esc(transform_helper(x, args...))
end


##############################################################################
##
## @based_on - summarize a grouping operation
##
##############################################################################

function based_on_helper(x, args...)
    with_args = :(DataFramesMeta.@with(_DF, DataFrames.DataFrame($(args...))))
    :( DataFrames.combine(map(_DF -> $with_args, $x)) )
end

"""
```julia
@based_on(g, i...)
```

Summarize a grouping operation

### Arguments

* `g` : a GroupedDataFrame
* `i...` : keyword arguments defining new columns

### Examples

```julia
d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x)
@based_on(g, nsum = sum(:n))
@based_on(g, x2 = 2 * :x, nsum = sum(:n))
```
"""
macro based_on(x, args...)
    # esc(:( DataFrames.based_on($x, _DF -> DataFramesMeta.@with(_DF, DataFrames.DataFrame($(args...)))) ))
    esc(based_on_helper(x, args...))
end


##############################################################################
##
## @by - grouping
##
##############################################################################

function by_helper(x, what, args...)
    with_args = :(DataFramesMeta.@with(_DF, DataFrames.DataFrame($(args...))))
    :( DataFrames.by($x, $what, _DF -> $with_args) )
end

"""
```julia
@by(d::AbstractDataFrame, cols, e...)
```

Split-apply-combine in one step.

### Arguments

* `d` : an AbstractDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `e` :  keyword arguments specifying new columns in terms of column groupings

### Returns

* `::DataFrame`

### Examples

```julia
df = DataFrame(a = rep(1:4, 2), b = rep(2:-1:1, 4), c = randn(8))
@by(df, :a, d = sum(:c))
@by(df, :a, d = 2 * :c)
@by(df, :a, c_sum = sum(:c), c_mean = mean(:c))
@by(df, :a, c = :c, c_mean = mean(:c))
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

expandargs(x) = x

function expandargs(e::Expr)
    if e.head == :quote && length(e.args) == 1
        return Expr(:kw, e.args[1], Expr(:quote, e.args[1]))
    else
        return e
    end
end

function expandargs(e::Tuple)
    return map(expandargs, e)
end

function Base.select(d::Union{AbstractDataFrame, Associative}; kwargs...)
    result = typeof(d)()
    for (k, v) in kwargs
        result[k] = v
    end
    return result
end


function select_helper(x, args...)
    select_args = :(select(_DF; $(expandargs(args)...)))
    quote
        let _DF = $x
            DataFramesMeta.@with(_DF, $select_args)
        end
    end
end



"""
```julia
@select(d, e...)
```

Select and transform columns.

### Arguments

* `d` : an AbstractDataFrame or Associative
* `e` :  keyword arguments specifying new columns in terms of existing columns
  or symbols to specify existing columns

### Returns

* `::AbstractDataFrame` or `::Associative`

### Examples

```julia
d = Dict(:s => 3, :y => 44, :d => 5)
@select(d, x = :y + :d, :s)

df = DataFrame(a = rep(1:4, 2), b = rep(2:-1:1, 4), c = randn(8))
@select(df, :c, :a)
@select(df, :c, x = :b + :c)
```
"""
macro select(x, args...)
    esc(select_helper(x, args...))
end


##############################################################################
##
## Extras for GroupedDataFrames
##
##############################################################################

combnranges(starts, ends) = [[starts[i]:ends[i] for i in 1:length(starts)]...;]

DataFrame(g::GroupedDataFrame) = g.parent[g.idx[combnranges(g.starts, g.ends)], :]

Base.getindex(gd::GroupedDataFrame, I::AbstractArray{Int}) = GroupedDataFrame(gd.parent,
                                                                              gd.cols,
                                                                              gd.idx,
                                                                              gd.starts[I],
                                                                              gd.ends[I])


##############################################################################
##
## Extras for easier handling of Arrays
##
##############################################################################

export P, PassThrough

type PassThrough{T} <: AbstractVector{T}
    x::AbstractVector{T}
end
const P = PassThrough

Base.size(x::PassThrough) = size(x.x)
Base.getindex(x::PassThrough, i) = getindex(x.x, i)

DataFrames.upgrade_vector(v::PassThrough) = v.x

end # module
