module DataFramesMeta

importall Base
export @with, @select

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

select_helper(d, arg) = :( $d[@with($d, $arg),:] )
select_helper(d, arg, moreargs...) = :( getindex($d, @with($d, $arg), $(moreargs...)) )

macro select(d, args...)
    esc(select_helper(d, args...))
end

end # module
