module DataFramesMeta

importall Base
importall DataFrames
using DataFrames

# Basics:
export @with, @ix, @where, @orderby, @transform, @by, @based_on, @select
export where, orderby, transform, select

include("compositedataframe.jl")
include("linqmacro.jl")
include("byrow.jl")


##############################################################################
##
## @with
##
##############################################################################

replace_syms(x, membernames) = x
function replace_syms(e::Expr, membernames)
    if e.head == :call && length(e.args) == 2 && e.args[1] == :^
        return e.args[2]
    elseif e.head == :.     # special case for :a.b
        return Expr(e.head, replace_syms(e.args[1], membernames),
                            typeof(e.args[2]) == Expr && e.args[2].head == :quote ? e.args[2] : replace_syms(e.args[2], membernames))
    elseif e.head != :quote
        return Expr(e.head, (isempty(e.args) ? e.args : map(x -> replace_syms(x, membernames), e.args))...)
    else
        if haskey(membernames, e.args[1])
            return membernames[e.args[1]]
        else
            a = gensym()
            membernames[e.args[1]] = a
            return a
        end
    end
end

function with_helper(d, body)
    membernames = Dict{Symbol, Symbol}()
    body = replace_syms(body, membernames)
    funargs = map(x -> :( getindex($d, $(Meta.quot(x))) ), collect(keys(membernames)))
    funname = gensym()
    return(:( function $funname($(collect(values(membernames))...)) $body end; $funname($(funargs...)) ))
end

macro with(d, body)
    esc(with_helper(d, body))
end


##############################################################################
##
## @ix - row and row/col selector
##
##############################################################################

ix_helper(d, arg) = :( let d = $d; $d[@with($d, $arg),:]; end )
ix_helper(d, arg, moreargs...) = :( let d = $d; getindex(d, @with(d, $arg), $(moreargs...)); end )

macro ix(d, args...)
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

where_helper(d, arg) = :( where($d, _DF -> @with(_DF, $arg)) )

macro where(d, arg)
    esc(where_helper(d, arg))
end


##############################################################################
##
## select - select columns
##
##############################################################################

select(d::AbstractDataFrame, arg) = d[ arg]


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

# I don't esc just the input because I want _DF to be visible to the user
macro orderby(d, args...)
    esc(:(let _D = $d;  DataFramesMeta.orderby(_D, _DF -> DataFramesMeta.@with(_DF, DataFramesMeta.orderbyconstructor(_D)($(args...)))); end))
end


##############################################################################
##
## transform & @transform
##
##############################################################################

function transform(d::Union(AbstractDataFrame, Associative); kwargs...)
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


function transform_helper(x, args...)
    # convert each kw arg value to: _DF -> @with(_DF, arg)
    newargs = [args...]
    for i in 1:length(args)
        newargs[i].args[2] = :( _DF -> @with(_DF, $(newargs[i].args[2]) ) )
    end
    :( transform($x, $(newargs...)) )
end

macro transform(x, args...)
    esc(transform_helper(x, args...))
end



##############################################################################
##
## @based_on - summarize a grouping operation
##
##############################################################################

macro based_on(x, args...)
    esc(:( DataFrames.based_on($x, _DF -> DataFramesMeta.@with(_DF, DataFrames.DataFrame($(args...)))) ))
end


##############################################################################
##
## @by - grouping
##
##############################################################################

macro by(x, what, args...)
    esc(:( DataFrames.by($x, $what, _DF -> DataFramesMeta.@with(_DF, DataFrames.DataFrame($(args...)))) ))
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
    res = [e...]
    for i in 1:length(res)
        res[i] = expandargs(e[i])
    end
    return res
end

function select(d::Union(AbstractDataFrame, Associative); kwargs...)
    result = typeof(d)()
    for (k, v) in kwargs
        result[k] = v
    end
    return result
end

macro select(x, args...)
    esc(:(let _DF = $x; DataFramesMeta.@with(_DF, select(_DF, $(DataFramesMeta.expandargs(args)...))); end))
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

end # module
