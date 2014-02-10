module DataFramesMeta

importall Base
using DataFrames

# Basics:
export @with, @select

# LINQ-style extras:
export @sub, orderby, transform, @transform, @by, @based_on, select


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
## @select - row and row/col selector
##
##############################################################################

select_helper(d, arg) = :( $d[@with($d, $arg),:] )
select_helper(d, arg, moreargs...) = :( getindex($d, @with($d, $arg), $(moreargs...)) )

macro select(d, args...)
    esc(select_helper(d, args...))
end


##############################################################################
##
## @sub - select rows
##
##############################################################################

sub_helper(arg) = :( x -> x[@with(x, $arg),:] )    # sets up a curry if only one argument
sub_helper(d, arg) = :( $d[@with($d, $arg),:] )

macro sub(d, arg...)
    esc(sub_helper(d, arg...))
end


##############################################################################
##
## select - select columns
##
##############################################################################

select(d::AbstractDataFrame, arg) = d[:, arg]
select(arg) = x -> select(x, arg)


##############################################################################
##
## orderby
##
##############################################################################

orderby(x::AbstractDataFrame, o) = sort(x, cols = o)
orderby(o) = x -> orderby(x, o)


##############################################################################
##
## transform & @transform
##
##############################################################################

function transform(d::AbstractDataFrame; kwargs...)
    result = copy(d)
    for (k, v) in kwargs
        result[k] = v
    end
    return result
end

macro transform(x, args...)
    esc(:(@with($x, transform($x, $(args...)))))
end


##############################################################################
##
## @based_on - summarize a grouping operation
##
##############################################################################

macro based_on(x, args...)
    esc(:( DataFrames.based_on($x, _DF -> @with(_DF, DataFrame($(args...)))) ))
end


##############################################################################
##
## @by - grouping
##
##############################################################################

macro by(x, what, args...)
    esc(:( by($x, $what, _DF -> @with(_DF, DataFrame($(args...)))) ))
end


end # module
